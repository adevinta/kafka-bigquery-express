package com.adevinta.bq.bqwriter

import java.time.Instant
import zio._
import zio.test._
import StagingRepositoryLive.filterDoneAvroFileRefs
import TestSupport.makeAvroDoneFileName
import TestSupport.makeAvroFileName
import com.adevinta.bq.shared.remotedir.AvroGcsFileName
import com.adevinta.bq.shared.remotedir.DoneGcsFileName
import com.adevinta.bq.shared.remotedir.LocalDirectory
import com.adevinta.bq.shared.remotedir.RemoteDirectory
import com.adevinta.bq.shared.remotedir.TempLocalDirectoryConfig
import com.adevinta.zc.metrics.Metrics
import com.adevinta.zc.metrics.MetricsTest

object StagingRepositorySpec extends ZIOSpecDefault {

  override def spec: Spec[TestEnvironment with Scope, Any] =
    suite("StagingRepositorySpec")(
      suite("StagingRepositorySpec RemoteDirectory Fixture")(
        test("finds nothing when the directory is empty") {
          for {
            gcsStagingRepository <- ZIO.service[StagingRepository]
            files <- gcsStagingRepository.listDoneAvroFiles()
          } yield assertTrue(files.isEmpty)
        },
        test("finds an avro file when matching done file is present") {
          for {
            remoteDir <- ZIO.service[RemoteDirectory]
            fileNames = List(
              "europe_topic.4.animals_dataset.cats_table.182.113832.avro",
              "europe_topic.4.animals_dataset.cats_table.182.113832.113999.done",
            )
            _ <- ZIO.foreachDiscard(fileNames)(makeEmptyFile(remoteDir, _))
            _ <- remoteDir.listFiles.runCollect
            gcsStagingRepository <- ZIO.service[StagingRepository]
            files <- gcsStagingRepository.listDoneAvroFiles()

          } yield {
            assertTrue(
              files.head.remoteFileRef.fileName.endsWith(
                "europe_topic.4.animals_dataset.cats_table.182.113832.avro"
              )
            )
          }
        },
        test(
          "finds only avro files with min schema id if multiple schema ids found for same topiceventypartitionid"
        ) {
          for {
            remoteDir <- ZIO.service[RemoteDirectory]
            fileNames = List(
              "europe_topic.4.animals_dataset.cats_table.182.113832.avro",
              "europe_topic.4.animals_dataset.cats_table.182.113832.113999.done",
              "europe_topic.4.animals_dataset.cats_table.180.113832.avro",
              "europe_topic.4.animals_dataset.cats_table.180.113832.113999.done",
              "europe_topic.0.stones_dataset.diamond_table.1053.2916495.avro",
              "europe_topic.0.stones_dataset.diamond_table.1053.2916495.2916510.done",
              "europe_topic.0.stones_dataset.diamond_table.450.2916495.avro",
              "europe_topic.0.stones_dataset.diamond_table.450.2916495.2916510.done"
            )
            _ <- ZIO.foreachDiscard(fileNames)(makeEmptyFile(remoteDir, _))
            _ <- remoteDir.listFiles.runCollect
            gcsStagingRepository <- ZIO.service[StagingRepository]
            files <- gcsStagingRepository.listDoneAvroFiles()

          } yield {
            assertTrue(
              files.exists(
                _.remoteFileRef.fileName.contains(
                  "europe_topic.4.animals_dataset.cats_table.180.113832.avro"
                )
              ),
              files.exists(
                _.remoteFileRef.fileName.contains(
                  "europe_topic.0.stones_dataset.diamond_table.450.2916495.avro"
                )
              )
            )
          }

        },
        test("does not find avro file when matching done file is missing") {
          for {
            remoteDir <- ZIO.service[RemoteDirectory]
            // NOTE: different partitions:
            _ <- makeEmptyFile(
              remoteDirectory = remoteDir,
              fileName = "europe_topic.4.animals_dataset.cats_table.182.113832.avro",
            )
            _ <- makeEmptyFile(
              remoteDirectory = remoteDir,
              fileName = "europe_topic.5.animals_dataset.cats_table.182.113832.113999.done",
            )
            gcsStagingRepository <- ZIO.service[StagingRepository]
            files <- gcsStagingRepository.listDoneAvroFiles()
          } yield assertTrue(files.isEmpty)
        },
      )
        .provideSome[Scope](
          TempLocalDirectoryConfig.layer,
          StagingRepositoryLive.layer,
          LocalDirectory.layer,
          MetricsTest.layer,
        ),
      suite("StagingRepositorySpec Enrichment Fixture")(
        test(s"filterDoneAvroFileRefs works when the list is empty") {
          val result = filterDoneAvroFileRefs(Chunk.empty)
          assertTrue(result.isEmpty)
        },
        test(s"filterDoneAvroFileRefs without corresponding Done FileRefs") {
          val avroRefWithDone: AvroGcsFileName =
            makeAvroFileName(
              topicName = "europe_topic",
              datasetName = "animals_dataset",
              tableName = "cats_table"
            )
          val doneRef: DoneGcsFileName =
            makeAvroDoneFileName(
              topicName = "europe_topic",
              datasetName = "animals_dataset",
              tableName = "cats_table"
            )
          // Following Avro File does not have any corresponding Done File
          val avroRefWithoutDone: AvroGcsFileName =
            makeAvroFileName(
              topicName = "australia_topic",
              datasetName = "animals_dataset",
              tableName = "dogs_table"
            )

          // result from  filterDoneAvroFileRefs should only contains the `avroRefWithDone`
          // avroRefWithoutDone will be filtered out (removed from) the result
          val result = StagingRepositoryLive.filterDoneAvroFileRefs(
            Chunk(avroRefWithDone, doneRef, avroRefWithoutDone)
          )

          assertTrue(
            result.size == 1,
            result.contains(avroRefWithDone),
            // result should not contain any Avro file that does not have corresponding done file.
            !result.contains(avroRefWithoutDone),
          )
        }
      ),
      suite("StagingRepositoryLive Done File Deletion")(
        test(
          s"should not delete done files that have highest offset"
        ) {
          // Given, we have the following done files and avro files in the staging directory.
          val done0 =
            makeAvroDoneFileName(
              topicName = "europe_topic",
              partition = 10,
              datasetName = "animals_dataset",
              tableName = "cats_table",
              firstOffset = 100, // It has the highest offset
              creationTime = Instant.now(), // This is a latest done file
            )
          val avro1 =
            makeAvroFileName(
              partition = 10,
              datasetName = "animals_dataset",
              tableName = "cats_table",
              firstOffset = 10,
              creationTime = Instant.EPOCH, // more than a month old
            )
          // done1 is more than a month old, however it has corresponding Avro file.
          // So it should not be deleted.
          val done1 =
            makeAvroDoneFileName(
              topicName = "europe_topic",
              partition = 10,
              datasetName = "animals_dataset",
              tableName = "cats_table",
              firstOffset = 10, // It has the highest offset
              creationTime = Instant.EPOCH, // more than a month old
            )

          for {
            now <- Clock.instant
            remoteDir <- ZIO.service[RemoteDirectory]
            metrics <- ZIO.service[Metrics]

            stagingRepo = StagingRepositoryLive(remoteDir, metrics.registry)
            fileChunks = Chunk(done0, avro1, done1)

            // When, `getOldDoneFilesToDelete` is invoked
            (processedDoneFile, doneFilesToBeDeleted, doneFilesToDeleteMap) =
              stagingRepo.getOldDoneFilesToDelete(
                fileChunks,
                now.minus(30.days),
              )
          } yield {
            // Then, there will be no done file that is marked for deletion.
            assertTrue(
              processedDoneFile == 1,
              doneFilesToBeDeleted == 0,
              doneFilesToDeleteMap.isEmpty,
              !doneFilesToDeleteMap.contains(done0.filePartitionId),
            )
          }
        },
        test(s"should not delete done files that have corresponding Avro files") {
          // Given, we have the following done files and avro files in the staging directory.
          val avro1 =
            makeAvroFileName(datasetName = "animals_dataset", tableName = "cats_table")
          val done1 =
            makeAvroDoneFileName(
              topicName = "europe_topic",
              datasetName = "animals_dataset",
              tableName = "cats_table"
            )
          // Following two done files does not have any corresponding avro files.
          val done2 =
            makeAvroDoneFileName(
              topicName = "europe_topic",
              partition = 10,
              datasetName = "animals_dataset",
              tableName = "dogs_table",
              firstOffset = 1,
              creationTime = Instant.now(),
            )
          val done3 =
            makeAvroDoneFileName(
              topicName = "europe_topic",
              partition = 10,
              datasetName = "animals_dataset",
              tableName = "dogs_table",
              firstOffset = 2,
              creationTime = Instant.now(),
            )
          for {
            now <- Clock.instant
            remoteDir <- ZIO.service[RemoteDirectory]
            metrics <- ZIO.service[Metrics]

            stagingRepo = StagingRepositoryLive(remoteDir, metrics.registry)
            fileChunks = Chunk(avro1, done1, done2, done3)
            // When`getOldDoneFilesToDelete` is invoked
            (processedDoneFile, doneFilesToBeDeleted, doneFilesToDeleteMap) =
              stagingRepo.getOldDoneFilesToDelete(
                fileChunks,
                now.minus(30.days),
              )
          } yield {
            // Then, files that have the highest offset or have a related avro file
            // will not be deleted. .
            assertTrue(
              processedDoneFile == 2,
              doneFilesToBeDeleted == 1,
              doneFilesToDeleteMap.contains(done2.filePartitionId),
              doneFilesToDeleteMap(done2.filePartitionId).contains(done2),
              // done1 should not be deleted as it has corresponding Avro file.
              !doneFilesToDeleteMap.contains(done1.filePartitionId),
              // done3 has the highest offset, so it should not be deleted.
              !doneFilesToDeleteMap(done3.filePartitionId).contains(done3),
            )
          }
        },
        test(
          s"should delete done files that are older than a month old even when they have highest offset"
        ) {
          // Given we have the following done files and avro files in the staging directory.
          val done0 =
            makeAvroDoneFileName(
              topicName = "europe_topic",
              partition = 10,
              "animals_dataset",
              "cats_table",
              firstOffset = 1,
              creationTime = Instant.EPOCH, // more than a month old
            )
          val avro1 =
            makeAvroFileName(
              topicName = "europe_topic",
              partition = 10,
              datasetName = "animals_dataset",
              tableName = "cats_table",
              firstOffset = 10,
              creationTime = Instant.EPOCH, // more than a month old
            )
          // done1 is more than a month old, however it has correspoindng Avro file.
          // So it should not be deleted.
          val done1 =
            makeAvroDoneFileName(
              topicName = "europe_topic",
              partition = 10,
              datasetName = "animals_dataset",
              tableName = "cats_table",
              firstOffset = 10,
              creationTime = Instant.EPOCH, // more than a month old
            )

          // Following two done Files does not have any corresponding avro File.
          val done2 =
            makeAvroDoneFileName(
              topicName = "europe_topic",
              partition = 10,
              datasetName = "animals_dataset",
              tableName = "gods_table",
              firstOffset = 1,
              creationTime = Instant
                .now()
                .minus(
                  31,
                  java.time.temporal.ChronoUnit.DAYS,
                ), // more than a month old
            )
          val done3 =
            makeAvroDoneFileName(
              topicName = "europe_topic",
              partition = 10,
              datasetName = "animals_dataset",
              tableName = "dogs_table",
              firstOffset = 2,
              creationTime = Instant
                .now()
                .minus(
                  31,
                  java.time.temporal.ChronoUnit.DAYS,
                ), // more than a month old
            )
          for {
            now <- Clock.instant
            remoteDir <- ZIO.service[RemoteDirectory]
            metrics <- ZIO.service[Metrics]

            stagingRepo = StagingRepositoryLive(remoteDir, metrics.registry)
            fileChunks = Chunk(done0, avro1, done1, done2, done3)

            // When `getOldDoneFilesToDelete` is invoked
            (processedDoneFile, doneFilesToBeDeleted, doneFilesToDeleteMap) =
              stagingRepo.getOldDoneFilesToDelete(
                fileChunks,
                now.minus(30.days),
              )
          } yield {
            // Then, all done files of partition 10 will be marked for deletion as they are more than a month old.
            assertTrue(
              processedDoneFile == 3,
              doneFilesToBeDeleted == 3,
              doneFilesToDeleteMap.contains(done2.filePartitionId),
              doneFilesToDeleteMap.contains(done3.filePartitionId),
              doneFilesToDeleteMap(done2.filePartitionId).contains(done2),
              // `done3` has the highest offset but it is more than a month old, so it should be deleted.
              doneFilesToDeleteMap(done3.filePartitionId).contains(done3),
              doneFilesToDeleteMap(done0.filePartitionId).contains(done0),
              // `done1` should not be deleted as it has corresponding Avro file (though it is more than a month old).
              !doneFilesToDeleteMap(done0.filePartitionId).contains(done1),
            )
          }
        },
      ).provideSome[Scope](
        LocalDirectory.layer,
        TempLocalDirectoryConfig.layer,
        MetricsTest.layer,
      ) @@ TestAspect.withLiveClock,
    )

  private def makeEmptyFile(
      remoteDirectory: RemoteDirectory,
      fileName: String,
  ): ZIO[RemoteDirectory, Throwable, Unit] =
    remoteDirectory.makeFile("staging/" + fileName, Array.emptyByteArray)

}
