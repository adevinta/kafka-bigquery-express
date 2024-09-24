package com.adevinta.bq.gcswriter

import com.adevinta.bq.gcswriter.config.StartOffsetRetrieverConfig
import com.adevinta.bq.shared.remotedir.AvroGcsFileName
import com.adevinta.bq.shared.remotedir.DoneGcsFileName
import com.adevinta.bq.shared.remotedir.GcsFileName
import com.adevinta.bq.shared.remotedir.RemoteDirectory
import com.adevinta.zc.metrics.Metrics
import io.prometheus.client.CollectorRegistry
import io.prometheus.client.Counter
import org.apache.kafka.common.TopicPartition
import zio._

trait StartOffsetRetriever {

  /** @param topicPartitions
    *   TopicPartitions for which the offset should be retrieved
    * @return
    *   a map with the starting offset for every topic partition
    *
    * side effect: it deletes all the .avro files in gcs cloud storage that do not have a
    * counterpart .done file
    */
  def getOffset(topicPartitions: Set[TopicPartition]): Task[Map[TopicPartition, Long]]
}

object StartOffsetRetrieverLive {
  private type Dependencies = StartOffsetRetrieverConfig with RemoteDirectory with Metrics

  def layer: ZLayer[Dependencies, Throwable, StartOffsetRetriever] = ZLayer {
    for {
      config <- ZIO.service[StartOffsetRetrieverConfig]
      remoteDirectory <- ZIO.service[RemoteDirectory]
      metrics <- ZIO.service[Metrics]
    } yield StartOffsetRetrieverLive(config, remoteDirectory, metrics.registry)
  }
}

//noinspection SimplifySleepInspection
final case class StartOffsetRetrieverLive(
    config: StartOffsetRetrieverConfig,
    remoteDirectory: RemoteDirectory,
    registry: CollectorRegistry
) extends StartOffsetRetriever {

  private val avroFilesDeletedCounter = Counter
    .build(
      "gcswriter_avrofilesdeletedcounter",
      "Deleted .avro files at startup that lacked a corresponding .done file."
    )
    .register(registry)

  override def getOffset(topicPartitions: Set[TopicPartition]): Task[Map[TopicPartition, Long]] = {
    // When we get assigned a partition, another process is very likely still processing earlier
    // records from that partition. By sleeping we give that process some time to finish writing
    // their done files.
    ZIO.sleep(config.waitTime) *>
      (
        for {
          relevantGcsFileNames <- listRelevantOdinFiles(topicPartitions)
          (avroFiles, doneFiles) = partitionToAvroAndDone(relevantGcsFileNames)
          _ <- deleteNotDoneAvroFiles(avroFiles, doneFiles)
          startingOffsets = calculateStartingOffsets(topicPartitions, doneFiles)
          _ <- ZIO.logInfo(
            s"Starting offset stats: ${startingOffsets.size} partitions (of ${topicPartitions.size}), " +
              s"zero offset found for ${startingOffsets.count(_._2 == 0L)} partitions"
          )
          _ <- ZIO.logDebug(s"All starting offsets: ${startingOffsets.mkString("; ")}")
        } yield startingOffsets
      )
  }

  private def listRelevantOdinFiles(
      topicPartitions: Set[TopicPartition]
  ): ZIO[Any, Throwable, Chunk[GcsFileName]] = {
    remoteDirectory
      .listFiles
      .mapChunks { fileRefs =>
        // Only keep the .avro and .done files for the given topic partitions
        fileRefs.flatMap { fileRef =>
          GcsFileName
            .parseFileName(fileRef)
            .toOption
            .filter(gcsFileName => topicPartitions.contains(gcsFileName.topicPartition))
        }
      }
      .runCollect
  }

  private def partitionToAvroAndDone(
      relevantGcsFileNames: Chunk[GcsFileName]
  ): (Set[AvroGcsFileName], Set[DoneGcsFileName]) = {
    val (avroFiles, doneFiles) = relevantGcsFileNames.partitionMap {
      case avroFile: AvroGcsFileName => Left(avroFile)
      case doneFile: DoneGcsFileName => Right(doneFile)
    }
    (avroFiles.toSet, doneFiles.toSet)
  }

  // Delete the .avro files without a .done counterpart
  private def deleteNotDoneAvroFiles(
      avroFiles: Set[AvroGcsFileName],
      doneFiles: Set[DoneGcsFileName]
  ): ZIO[Any, Throwable, Unit] = {
    val avroToBeDeleted = getAvroToBeDeleted(avroFiles, doneFiles)
    val toBeDeletedFileNames = Chunk.fromIterable(avroToBeDeleted.map(_.remoteFileRef.fileName))
    remoteDirectory.deleteFiles(toBeDeletedFileNames)
      .as(avroFilesDeletedCounter.inc(toBeDeletedFileNames.size))
  }

  private def getAvroToBeDeleted(
      avroFiles: Set[AvroGcsFileName],
      doneFiles: Set[DoneGcsFileName]
  ): Set[AvroGcsFileName] = {
    val avroByPartitionSegment: Map[String, AvroGcsFileName] =
      avroFiles.map(avroFile => avroFile.filePartitionSegmentId -> avroFile).toMap
    val avroCompleted: Set[AvroGcsFileName] = doneFiles.flatMap { doneFile =>
      avroByPartitionSegment.get(doneFile.filePartitionSegmentId)
    }
    avroFiles -- avroCompleted
  }

  private def calculateStartingOffsets(
      topicPartitions: Set[TopicPartition],
      doneFiles: Set[DoneGcsFileName]
  ): Map[TopicPartition, Long] = {
    val zeroOffsets = topicPartitions.iterator.map(_ -> 0L).toMap
    doneFiles
      .foldLeft(zeroOffsets) { (offsetsMap, doneFile) =>
        val key = doneFile.topicPartition
        // The starting offset is the last processed + 1
        val maxOffset = (doneFile.lastOffset + 1).max(offsetsMap(key))
        offsetsMap.updated(key, maxOffset)
      }
  }

}
