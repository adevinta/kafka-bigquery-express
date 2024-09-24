package com.adevinta.bq.bqwriter

import com.adevinta.bq.bqwriter.config.JobCoordinatorConfig
import com.adevinta.bq.bqwriter.job.BigQueryJob
import com.adevinta.bq.bqwriter.job.EnrichedBigQueryJobs
import com.adevinta.bq.bqwriter.job.JobError
import com.adevinta.bq.bqwriter.job.JobStatus
import com.adevinta.bq.bqwriter.mocks.BigQueryJobServiceMock
import com.adevinta.bq.bqwriter.mocks.JobsRepositoryMock
import com.adevinta.bq.bqwriter.mocks.StagingRepositoryMock

import java.time.Instant
import zio._
import zio.mock.Expectation._
import zio.test.Assertion._
import zio.test._
import TestSupport.makeAvroFileName
import com.adevinta.bq.shared.remotedir.AvroGcsFileName
import com.adevinta.bq.shared.remotedir.RemoteFileRef
import com.adevinta.bq.shared.remotedir.TableId
import com.adevinta.zc.metrics.MetricsTest

object JobCoordinatorLiveSpec extends ZIOSpecDefault {

  private val JobCoordinatorConfigLayer: ZLayer[Any, Nothing, JobCoordinatorConfig] =
    ZLayer.succeed(
      JobCoordinatorConfig(
        jobSubmissionInterval = 5.minutes,
        jobCheckInterval = 30.seconds,
        maxRetries = 2,
        jobPrefix = Fixtures.JobPrefix,
        bigQueryJobMaxRuntime = 10.minutes,
      )
    )

  override def spec: Spec[TestEnvironment with Scope, Any] = {
    suite("JobCoordinatorLiveSpec")(
      suite("JobCoordinatorLiveSpec reSubmitNewJobs")(
        test("schedules no jobs given empty state") {
          val initialState = EnrichedBigQueryJobs.empty
          val expectations =
            BigQueryJobServiceMock.RunningJobsWithUpdatedStatus(
              equalTo((Fixtures.JobPrefix, Chunk.empty)),
              value(Chunk.empty)
            )

          val sut =
            for {
              jobCoordinator <- ZIO.serviceWith[JobCoordinator](_.asInstanceOf[JobCoordinatorLive])
              endState <- jobCoordinator.reSubmitNewJobs(initialState)
            } yield {
              assertTrue(endState.size == 0)
            }

          sut.provide(
            JobCoordinatorLive.layer,
            JobCoordinatorConfigLayer,
            expectations,
            StagingRepositoryMock.empty,
            JobsRepositoryMock.empty,
            MetricsTest.layer
          )
        },
        test("reschedules jobs in the NEW state") {
          val runningJob1 = makeBigQueryJob(tableId = tableId1, jobStatus = JobStatus.New)
          val runningJob2 = makeBigQueryJob(tableId = tableId2, jobStatus = JobStatus.New)
          val runningJob3 = makeBigQueryJob(tableId = tableId3, jobStatus = JobStatus.Running)
          val initialState = EnrichedBigQueryJobs.make(
            Chunk(runningJob1, runningJob2, runningJob3)
          )

          val expectations =
            BigQueryJobServiceMock.RunningJobsWithUpdatedStatus(
              equalTo((Fixtures.JobPrefix, Chunk(runningJob1, runningJob2, runningJob3))),
              value(
                Chunk(
                  runningJob1.copy(jobStatus = JobStatus.Running), // Job1 is running after all
                  runningJob2, // Job2 is still NEW, needs to be re-submitted
                  runningJob3 // Job3 is still running
                )
              )
            ) ++
              jobsStateSaved ++
              expectJobSubmitted(tableId2)

          val sut =
            for {
              jobCoordinator <- ZIO.serviceWith[JobCoordinator](_.asInstanceOf[JobCoordinatorLive])
              endState <- jobCoordinator.reSubmitNewJobs(initialState)
            } yield {
              // Note that the endState does not change when re-submitting jobs.
              assertTrue(endState == initialState)
            }

          sut.provide(
            JobCoordinatorLive.layer,
            JobCoordinatorConfigLayer,
            expectations,
            StagingRepositoryMock.empty,
            MetricsTest.layer
          )
        }
      ),
      suite("JobCoordinatorLiveSpec scheduleJobs")(
        test("schedules no jobs given no avro files") {
          val expectations =
            givenThereAreNoAvroFiles

          val startState = EnrichedBigQueryJobs.empty

          val sut =
            for {
              jobCoordinator <- ZIO.serviceWith[JobCoordinator](_.asInstanceOf[JobCoordinatorLive])
              endState <- jobCoordinator.scheduleJobs(startState)
            } yield {
              assertTrue(endState.size == 0)
            }

          sut.provide(
            JobCoordinatorLive.layer,
            expectations,
            JobCoordinatorConfigLayer,
            BigQueryJobServiceMock.empty,
            JobsRepositoryMock.empty,
            MetricsTest.layer
          )
        },
        test("schedules no jobs given new avro files that are handled by an existing job") {
          val expectations = givenSomeAvroFiles(Chunk(avroFile2000))
          val runningJob = makeBigQueryJob(fileRefs = Vector(avroFile2000.remoteFileRef))
          val startState = EnrichedBigQueryJobs.make(Chunk(runningJob))

          val sut =
            for {
              jobCoordinator <- ZIO.serviceWith[JobCoordinator](_.asInstanceOf[JobCoordinatorLive])
              endState <- jobCoordinator.scheduleJobs(startState)
            } yield {
              assertTrue(endState == startState)
            }

          sut.provide(
            JobCoordinatorLive.layer,
            expectations,
            JobCoordinatorConfigLayer,
            BigQueryJobServiceMock.empty,
            JobsRepositoryMock.empty,
            MetricsTest.layer
          )
        },
        test("schedules new job given new avro files that are not handler by an existing job") {
          val expectations =
            givenSomeAvroFiles(Chunk(avroFile2000)) ++
              jobsStateSaved ++
              expectJobSubmitted(avroFile2000.tableId)

          val startState = EnrichedBigQueryJobs.make(Chunk.empty)

          val sut =
            for {
              jobCoordinator <- ZIO.serviceWith[JobCoordinator](_.asInstanceOf[JobCoordinatorLive])
              endState <- jobCoordinator.scheduleJobs(startState)
            } yield {
              assertTrue(
                endState.size == 1,
                endState.jobs.headOption.exists { job =>
                  job.jobId.uid.startsWith(
                    s"${Fixtures.JobPrefix}_${avroFile2000.tableId.datasetName}_${avroFile2000.tableId.tableName}_"
                  ) &&
                  job.tableId.datasetName == avroFile2000.datasetName &&
                  job.tableId == avroFile2000.tableId &&
                  job.fileRefs == Vector(avroFile2000.remoteFileRef) &&
                  job.jobStatus == JobStatus.New &&
                  job.creationTime == Instant.EPOCH &&
                  job.retryCount == 0
                }
              )
            }

          sut.provide(
            JobCoordinatorLive.layer,
            expectations,
            JobCoordinatorConfigLayer,
            MetricsTest.layer
          )
        }
      ),
      suite("JobCoordinatorLiveSpec checkJobs")(
        test("does nothing when there are no avro files and no running jobs") {
          val expectations =
            BigQueryJobServiceMock.RunningJobsWithUpdatedStatus(
              equalTo((Fixtures.JobPrefix, Chunk.empty)),
              value(Chunk.empty)
            )

          val startState = EnrichedBigQueryJobs.empty

          val sut =
            for {
              jobCoordinator <- ZIO.serviceWith[JobCoordinator](_.asInstanceOf[JobCoordinatorLive])
              endState <- jobCoordinator.checkJobs(startState)
            } yield {
              assertTrue(endState.size == 0)
            }

          sut.provide(
            JobCoordinatorLive.layer,
            expectations,
            JobCoordinatorConfigLayer,
            JobsRepositoryMock.empty,
            StagingRepositoryMock.empty,
            MetricsTest.layer
          )
        },
        test("deletes avro files for completed jobs") {
          val runningJob1 = makeBigQueryJob(
            tableId = tableId1,
            jobStatus = JobStatus.Running,
            fileRefs = Vector(avroFile1000.remoteFileRef)
          )
          val runningJob2 = makeBigQueryJob(
            tableId = tableId2,
            jobStatus = JobStatus.Running,
            fileRefs = Vector(avroFile2000.remoteFileRef)
          )
          val startState = EnrichedBigQueryJobs.make(Chunk(runningJob1, runningJob2))

          val expectations =
            BigQueryJobServiceMock.RunningJobsWithUpdatedStatus(
              equalTo((Fixtures.JobPrefix, Chunk(runningJob1, runningJob2))),
              value(
                Chunk(
                  runningJob1.copy(jobStatus = JobStatus.Running), // Job1 is still running
                  runningJob2.copy(jobStatus = JobStatus.Success), // Job2 completed
                )
              )
            ) ++
              StagingRepositoryMock.DeleteFiles(
                equalTo(Chunk.from(runningJob2.fileRefs)),
                unit
              ) ++
              jobsStateSaved

          val sut =
            for {
              jobCoordinator <- ZIO.serviceWith[JobCoordinator](_.asInstanceOf[JobCoordinatorLive])
              endState <- jobCoordinator.checkJobs(startState)
            } yield {
              assertTrue(
                endState.tableIdsInActiveLoadJobs == Set(tableId1)
              )
            }

          sut.provide(
            JobCoordinatorLive.layer,
            expectations,
            JobCoordinatorConfigLayer,
            MetricsTest.layer
          )
        },
        test("schedule new jobs for failed jobs") {
          val runningJob1 = makeBigQueryJob(
            tableId = tableId1,
            jobStatus = JobStatus.Running,
            fileRefs = Vector(avroFile1000.remoteFileRef)
          )
          val runningJob2 = makeBigQueryJob(
            tableId = tableId2,
            jobStatus = JobStatus.Running,
            fileRefs = Vector(avroFile2000.remoteFileRef)
          )
          val startState = EnrichedBigQueryJobs.make(Chunk(runningJob1, runningJob2))

          val failedState = JobStatus.Failed(new JobError(Some("~failed job"), None, None, None))
          val expectations =
            BigQueryJobServiceMock.RunningJobsWithUpdatedStatus(
              equalTo((Fixtures.JobPrefix, Chunk(runningJob1, runningJob2))),
              value(
                Chunk(
                  runningJob1.copy(jobStatus = JobStatus.Running), // Job1 is still running
                  runningJob2.copy(jobStatus = failedState), // Job2 failed, has to be retried
                )
              )
            ) ++
              jobsStateSaved ++
              expectJobSubmitted(tableId2)

          val sut =
            for {
              jobCoordinator <- ZIO.serviceWith[JobCoordinator](_.asInstanceOf[JobCoordinatorLive])
              endState <- jobCoordinator.checkJobs(startState)
            } yield {
              val job1 = endState.jobs.find(_.tableId == tableId1)
              val job2 = endState.jobs.find(_.tableId == tableId2)
              assertTrue(
                job1.contains(runningJob1),
                job2.exists(job => job.retryCount == 1 && job.isNew)
              )
            }

          sut.provide(
            JobCoordinatorLive.layer,
            expectations,
            StagingRepositoryMock.empty,
            JobCoordinatorConfigLayer,
            MetricsTest.layer
          )
        },
        test("fail on jobs with retryCount > MaxRetryCount") {
          val runningJob1 = makeBigQueryJob(
            tableId = tableId1,
            jobStatus = JobStatus.Running,
            fileRefs = Vector(avroFile1000.remoteFileRef),
            retryCount = 2
          )
          val startState = EnrichedBigQueryJobs.make(Chunk(runningJob1))

          val failedState = JobStatus.Failed(new JobError(Some("~failed job"), None, None, None))
          val expectations =
            BigQueryJobServiceMock.RunningJobsWithUpdatedStatus(
              equalTo((Fixtures.JobPrefix, Chunk(runningJob1))),
              value(
                Chunk(
                  runningJob1.copy(jobStatus = failedState), // Job failed again, too often
                )
              )
            )

          val sut =
            for {
              jobCoordinator <- ZIO.serviceWith[JobCoordinator](_.asInstanceOf[JobCoordinatorLive])
              result <- jobCoordinator.checkJobs(startState).exit
            } yield {
              assert(result)(fails(isSubtype[JobCoordinator.TooManyRetries.type](anything)))
            }

          sut.provide(
            JobCoordinatorLive.layer,
            expectations,
            JobsRepositoryMock.empty,
            StagingRepositoryMock.empty,
            JobCoordinatorConfigLayer,
            MetricsTest.layer
          )
        },
        test("fail on jobs with long running job") {
          val runningJob1 = makeBigQueryJob(
            jobStatus = JobStatus.Running,
            creationTime = Instant.EPOCH
          )
          val startState = EnrichedBigQueryJobs.make(Chunk(runningJob1))
          val expectations =
            BigQueryJobServiceMock.RunningJobsWithUpdatedStatus(
              equalTo((Fixtures.JobPrefix, Chunk(runningJob1))),
              value(Chunk(runningJob1))
            )

          val sut =
            for {
              // Job 1 is running for too long
              _ <- TestClock.setTime(Instant.EPOCH.plus(11.minutes))
              jobCoordinator <- ZIO.serviceWith[JobCoordinator](_.asInstanceOf[JobCoordinatorLive])
              result <- jobCoordinator.checkJobs(startState).exit
            } yield {
              assert(result)(fails(isSubtype[JobCoordinator.JobRunningTooLong.type](anything)))
            }

          sut.provide(
            JobCoordinatorLive.layer,
            expectations,
            JobsRepositoryMock.empty,
            StagingRepositoryMock.empty,
            JobCoordinatorConfigLayer,
            MetricsTest.layer
          )
        },
        test("do not fail on job that completed just before the max runtime") {
          val runningJob1 = makeBigQueryJob(
            jobStatus = JobStatus.Running,
            creationTime = Instant.EPOCH
          )
          val startState = EnrichedBigQueryJobs.make(Chunk(runningJob1))
          val expectations =
            BigQueryJobServiceMock.RunningJobsWithUpdatedStatus(
              equalTo((Fixtures.JobPrefix, Chunk(runningJob1))),
              value(Chunk(runningJob1.copy(jobStatus = JobStatus.Success))) // job is done now
            ) ++
              StagingRepositoryMock.DeleteFiles(anything, unit) ++
              jobsStateSaved

          val sut =
            for {
              // Job 1 finished just in time
              _ <- TestClock.setTime(Instant.EPOCH.plus(11.minutes))
              jobCoordinator <- ZIO.serviceWith[JobCoordinator](_.asInstanceOf[JobCoordinatorLive])
              endState <- jobCoordinator.checkJobs(startState)
            } yield {
              assertTrue(endState.size == 0)
            }

          sut.provide(
            JobCoordinatorLive.layer,
            expectations,
            JobCoordinatorConfigLayer,
            MetricsTest.layer
          )
        }
      ),
      suite("JobCoordinatorConfigLayer selectCompatibleAvroFilesNew")(
        test("returns 0 files if there are no avro files") {
          val result = JobCoordinatorLive.selectCompatibleAvroFilesNew(Chunk.empty)
          assertTrue(result.isEmpty)
        },
        test("limits the number of avro files to maximum URI count") {
          val avroFiles = Chunk.tabulate(10123)(i => makeAvroFileName(firstOffset = i * 100))
          val result = JobCoordinatorLive.selectCompatibleAvroFilesNew(avroFiles)
          val testResult = result == avroFiles.take(10000)
          assertTrue(testResult)
        },
        test("only takes files with the lowest schema id") {
          val avroFiles843 =
            Chunk.tabulate(10)(i => makeAvroFileName(firstOffset = i * 100, schemaId = 843))
          val avroFiles1022 =
            Chunk.tabulate(10)(i => makeAvroFileName(firstOffset = (10 + i) * 100, schemaId = 1022))
          val avroFiles = avroFiles843 ++ avroFiles1022
          val result = JobCoordinatorLive.selectCompatibleAvroFilesNew(avroFiles)
          val testResult = result == avroFiles843
          assertTrue(testResult)
        },
        test("only takes files with the lowest schema id for many files") {
          val avroFiles843 =
            Chunk.tabulate(10123)(i => makeAvroFileName(firstOffset = i * 100, schemaId = 843))
          val avroFiles1022 = Chunk.tabulate(10)(i =>
            makeAvroFileName(firstOffset = (1023 + i) * 100, schemaId = 1022)
          )
          val avroFiles = avroFiles843 ++ avroFiles1022
          val result = JobCoordinatorLive.selectCompatibleAvroFilesNew(avroFiles)
          val testResult = result == avroFiles843.take(10000)
          assertTrue(testResult)
        },
      )
    )
  }

  private def givenThereAreNoAvroFiles =
    StagingRepositoryMock.ListDoneAvroFiles(value(Chunk.empty[AvroGcsFileName]))

  private def givenSomeAvroFiles(avroFiles: Chunk[AvroGcsFileName]) =
    StagingRepositoryMock.ListDoneAvroFiles(value(avroFiles))

  private def expectJobSubmitted(tableId: TableId) =
    BigQueryJobServiceMock.SubmitJobs(
      hasSize[BigQueryJob](equalTo(1)) &&
        exists(hasField[BigQueryJob, TableId]("tableId", _.tableId, equalTo(tableId))),
      unit
    )

  private def jobsStateSaved =
    JobsRepositoryMock.SaveJobs(anything, unit)

  // Mock and Expectation Setup
  private val avroFile3000 = makeAvroFileName(
    datasetName = Fixtures.testDatasetName,
    tableName = Fixtures.testTableName,
    firstOffset = 3000,
  )
  private val avroFile2000: AvroGcsFileName = makeAvroFileName(
    datasetName = Fixtures.testDatasetName,
    tableName = Fixtures.testTableName,
    firstOffset = 2000,
  )
  private val avroFile1000: AvroGcsFileName = makeAvroFileName(
    datasetName = Fixtures.testDatasetName,
    tableName = Fixtures.testTableName,
    firstOffset = 1000,
  )

  private val tableId1: TableId = TableId(
    Fixtures.testDatasetName,
    Fixtures.testTableName,
  )
  private val tableId2: TableId =
    TableId("stone_dataset", "diamond_table")
  private val tableId3: TableId =
    TableId("stone_dataset", "quartz_table")

  private def makeBigQueryJob(
      tableId: TableId = tableId1,
      fileRefs: Vector[RemoteFileRef] = Vector(avroFile3000.remoteFileRef),
      jobStatus: JobStatus = JobStatus.Success,
      creationTime: Instant = Instant.EPOCH,
      retryCount: Int = 0,
  ): BigQueryJob = BigQueryJob(
    jobId = Fixtures.jobId(tableId1),
    tableId = tableId,
    fileRefs = fileRefs,
    jobStatus = jobStatus,
    creationTime = creationTime,
    retryCount = retryCount
  )

}
