package com.adevinta.bq.bqwriter

import com.adevinta.bq.shared.remotedir.AvroGcsFileName
import com.adevinta.bq.shared.remotedir.DoneGcsFileName
import com.adevinta.bq.shared.remotedir.GcsFileName
import com.adevinta.bq.shared.remotedir.RemoteDirectory
import com.adevinta.bq.shared.remotedir.RemoteFileRef

import java.time.Instant
import scala.math.Ordered.orderingToOrdered
import io.prometheus.client.CollectorRegistry
import io.prometheus.client.Counter
import io.prometheus.client.Gauge
import zio._
import zio.prelude.ForEachOps
import com.adevinta.zc.metrics.Metrics

/** StagingRepository lists the Avro files available at the `staging` [[RemoteDirectory]] and
  * deletes files.
  */
trait StagingRepository {

  /** Lists Avro files from the staging directory that are available for loading into BigQuery. */
  def listDoneAvroFiles(): ZIO[Any, Throwable, Chunk[AvroGcsFileName]]

  /** Deletes files from the staging directory. */
  def deleteFiles(refs: Chunk[RemoteFileRef]): ZIO[Any, Throwable, Unit]

  def runDoneFileCleanup(): ZIO[Any, Throwable, Unit]
}

object StagingRepositoryLive {
  private type Dependencies = RemoteDirectory with Metrics

  val layer: ZLayer[Dependencies, Any, StagingRepositoryLive] = ZLayer {
    for {
      remoteDirectory <- ZIO.service[RemoteDirectory]
      metrics <- ZIO.service[Metrics]
    } yield StagingRepositoryLive(
      remoteDirectory,
      metrics.registry,
    )
  }

  /** A filter that keeps avro files that have a corresponding `done` file. */
  // Note: Access is package private for testing purposes.
  private[bqwriter] def filterDoneAvroFileRefs(
      allFiles: Chunk[GcsFileName]
  ): Chunk[AvroGcsFileName] = {
    val (avroFiles, doneFiles) =
      allFiles.partitionMap {
        case avroFile: AvroGcsFileName => Left(avroFile)
        case doneFile: GcsFileName     => Right(doneFile)
      }
    val donePartitionSegmentIds: Set[String] = doneFiles.map(_.filePartitionSegmentId).toSet
    val avroFilesWithDoneFiles: Chunk[AvroGcsFileName] =
      avroFiles.filter(avroFile =>
        donePartitionSegmentIds.contains(avroFile.filePartitionSegmentId)
      )

    avroFilesWithDoneFiles
  }
}

final case class StagingRepositoryLive(
    remoteDirectory: RemoteDirectory,
    registry: CollectorRegistry,
) extends StagingRepository {
  import StagingRepositoryLive._

  private val stagingFilesGauge = Gauge
    .build(
      "bqwriter_stagingfiles",
      "Number of files in the staging area, these count both .avro and .done files, before being loaded to BQ",
    )
    .register(registry)

  private val stagingFilesDeletedCounter = Counter
    .build(
      "bqwriter_stagingfilesdeleted",
      "Number of files deleted from the staging area (counts both .avro and .done files)",
    )
    .register(registry)

  override def listDoneAvroFiles(): ZIO[Any, Throwable, Chunk[AvroGcsFileName]] =
    for {
      allRemoteFiles <- remoteDirectory.listFiles
        .mapChunks { fileRefs =>
          fileRefs.flatMap(fileRef => GcsFileName.parseFileName(fileRef).toOption)
        }
        .runCollect
      _ <- ZIO.succeed(stagingFilesGauge.set(allRemoteFiles.size))
      doneAvroFiles = filterDoneAvroFileRefs(allRemoteFiles)
    } yield doneAvroFiles

  override def deleteFiles(refs: Chunk[RemoteFileRef]): ZIO[Any, Throwable, Unit] = {
    for {
      _ <- ZIO.logInfo(s"Deleting ${refs.size} files from staging")
      _ <- remoteDirectory.deleteFiles(refs.map(_.fileName))
      _ <- ZIO.succeed(stagingFilesDeletedCounter.inc(refs.size))
    } yield ()
  }

  override def runDoneFileCleanup(): ZIO[Any, Throwable, Unit] =
    deleteOldDoneFile()
      .schedule(Schedule.spaced(9.minutes).jittered)
      .unit

  /** Deletes the old `done` file from the staging directory.
    *
    * Following done files are deleted:
    *   - A Done file that does not have any corresponding Avro file in the staging directory AND
    *   - That is more than a month old || there is a newer Done file for the same partition.
    */
  // package private for testing
  private[bqwriter] def deleteOldDoneFile(): ZIO[Any, Throwable, Unit] = {
    val doneFileDeleteProcess: ZIO[Any, Throwable, Unit] =
      for {
        now <- Clock.instant
        _ <- ZIO.logDebug(s"Starting Done File Cleanup process.")
        allFiles <- remoteDirectory
          .listFiles
          .mapChunks { fileRefs =>
            fileRefs.flatMap(fileRef => GcsFileName.parseFileName(fileRef).toOption)
          }
          .runCollect

        // Gets the done file that can be deleted
        (processedDoneFilesCount, doneFilesToBeDeletedCount, doneFilesToDeleteMap) =
          getOldDoneFilesToDelete(
            allFiles,
            // Done file is valid for the duration of 1 month
            now.minus(30.days),
          )
        // Performs delete operation on the done files in `doneFilesToDeleteMap`
        _ <- ZIO.foreachParDiscard(doneFilesToDeleteMap) { case (_, filesToDelete) =>
          remoteDirectory.deleteFiles(filesToDelete.map(_.remoteFileRef.fileName)) <*
            ZIO.succeed(stagingFilesDeletedCounter.inc(filesToDelete.size))
        }
        _ <- ZIO.logInfo {
          s"Found $processedDoneFilesCount Done files at the staging remote directory, among which, $doneFilesToBeDeletedCount Done files have been deleted."
        }
      } yield ()
    doneFileDeleteProcess.absorb.logError.ignore
  }

  // package private for testing
  private[bqwriter] def getOldDoneFilesToDelete(
      files: Chunk[GcsFileName],
      durationDoneFileValid: Instant,
  ): (Int, Int, Map[String, Chunk[GcsFileName]]) = {
    var processedDoneFilesCount = 0
    var doneFilesToBeDeletedCount = 0
    val doneFilesToBeDeletedMap: Map[String, Chunk[GcsFileName]] =
      files
        .groupBy(_.filePartitionId)
        .map { case (topicPartitionId, doneAndAvroFiles) =>
          val sortedDoneFilesByFirstOffset: Chunk[GcsFileName] =
            getAlreadyProcessedDoneFiles(doneAndAvroFiles)
              .sortBy(-_.firstOffset)
          // Increase the Processed Done File found
          processedDoneFilesCount = processedDoneFilesCount + sortedDoneFilesByFirstOffset.size
          if (sortedDoneFilesByFirstOffset.isEmpty)
            (topicPartitionId, sortedDoneFilesByFirstOffset)
          else {
            // Note, the first (`head`) element in the list has the highest offset.
            val doneFileWithHighestStartOffset: GcsFileName = sortedDoneFilesByFirstOffset.head
            val doneFileWithHighestStartOffsetCreationTime: Instant =
              doneFileWithHighestStartOffset
                .remoteFileRef
                .creationTime
            val filesToBeDeleted: Chunk[GcsFileName] =
              if (doneFileWithHighestStartOffsetCreationTime > durationDoneFileValid) {
                // We do not delete the done file with the highest offset as it is less than a month old.
                sortedDoneFilesByFirstOffset.drop(1)
              } else {
                sortedDoneFilesByFirstOffset
              }
            // Update the count of Done Files to be deleted
            doneFilesToBeDeletedCount = doneFilesToBeDeletedCount + filesToBeDeleted.size
            (topicPartitionId, filesToBeDeleted)
          }
        }
        .filter(_._2.nonEmpty)
    (processedDoneFilesCount, doneFilesToBeDeletedCount, doneFilesToBeDeletedMap)
  }

  // Returns the Done files for which there is no corresponding Avro file in the staging
  private def getAlreadyProcessedDoneFiles(
      files: Chunk[GcsFileName]
  ): Chunk[GcsFileName] = {
    val (avroFiles, doneFiles) = files.partitionMap {
      case avroFile: AvroGcsFileName => Left(avroFile)
      case doneFile: DoneGcsFileName => Right(doneFile)
    }
    val avroFilesSet = avroFiles.map(_.filePartitionSegmentId).toSet
    val doneFilesMap = doneFiles.map(file => file.filePartitionSegmentId -> file).toMap

    // Removes all the Done files that have a corresponding Avro file in the staging
    (doneFilesMap -- avroFilesSet).values.toChunk
  }
}
