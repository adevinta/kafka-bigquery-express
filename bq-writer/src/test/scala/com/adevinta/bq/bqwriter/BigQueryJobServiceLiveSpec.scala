package com.adevinta.bq.bqwriter

import com.adevinta.bq.bqwriter.config.BigQueryConfig
import com.adevinta.bq.bqwriter.config.JobsRepositoryConfig
import com.adevinta.bq.bqwriter.job.BigQueryJob
import com.adevinta.bq.bqwriter.job.JobStatus
import com.adevinta.bq.shared.remotedir.BucketTest
import com.adevinta.bq.shared.remotedir.DefaultGcpCredentials
import com.adevinta.bq.shared.remotedir.GcpProjectIds
import com.adevinta.bq.shared.remotedir.GcsRemoteDirectory
import com.adevinta.bq.shared.remotedir.GcsRemoteDirectoryConfig
import com.adevinta.bq.shared.remotedir.RemoteDirectory
import com.adevinta.bq.shared.remotedir.RemoteFileRef
import com.adevinta.bq.shared.remotedir.{TableId => BQTableId}
import com.adevinta.zc.metrics.Metrics
import com.adevinta.zc.metrics.MetricsTest
import com.google.cloud.bigquery._
import zio._
import zio.stream.ZSink
import zio.stream.ZStream
import zio.test.TestAspect.ifEnv
import zio.test.TestAspect.withLiveClock
import zio.test.Spec
import zio.test.TestEnvironment
import zio.test.ZIOSpecDefault
import zio.test.assertTrue

import java.time.Instant

/** This is an integration test, it requires access to bigquery. See the readme.md. */
object BigQueryJobServiceLiveSpec extends ZIOSpecDefault {
  private val testDatasetName = s"testdataset-" + java.util.UUID.randomUUID().toString
  private val testTopicName = "test_topic"
  private val tableName: String = "loadjob"
  private val testTableId = BQTableId(
    testDatasetName,
    tableName
  )

  private val bigQueryConfigLayer: ZLayer[Any, Throwable, BigQueryConfig] =
    ZLayer.succeed(
      BigQueryConfig(
        projectId = GcpProjectIds.IntegrationProjectId,
        gcpCredentials = DefaultGcpCredentials,
        partitionRetentionInDays = 3,
        location = "europe-west4"
      )
    )

  private val bigQueryLayer: ZLayer[Scope & BigQueryConfig, Throwable, BigQuery] =
    ZLayer {
      for {
        config <- ZIO.service[BigQueryConfig]
        bigQuery = BigQueryOptions.newBuilder()
          .setProjectId(config.projectIdString)
          .setLocation(config.location)
          .build()
          .getService

        testDatasetName = testTableId.datasetName
        testTableName = testTableId.tableName
        _ <- ZIO.acquireRelease {
          ZIO.attemptBlocking {
            val datasetInfo =
              DatasetInfo.newBuilder(testDatasetName).setLocation(config.location).build()
            val dataset = bigQuery.create(datasetInfo)
            (bigQuery, dataset)
          }
        } { case (_, dataset) =>
          // Cleanup: Delete dataset, table, and all associated data
          ZIO.attemptBlocking {
            val tableId = TableId.of(dataset.getDatasetId.getDataset, testTableName)
            bigQuery.delete(tableId) // Delete table
            dataset.delete() // Delete dataset
          }.ignore
        }
      } yield bigQuery
    }

  private val GcsRemoveDirectoryTestConfig: GcsRemoteDirectoryConfig =
    GcsRemoteDirectoryConfig(GcpProjectIds.IntegrationProjectId, DefaultGcpCredentials)

  private val GcsRemoteDirectoryTestConfigLayer: ZLayer[Any, Nothing, GcsRemoteDirectoryConfig] =
    ZLayer.succeed(GcsRemoveDirectoryTestConfig)

  override val spec: Spec[TestEnvironment with Scope, Any] =
    suite("BigQueryJobServiceLiveSpec")(
      test("submits jobs and fetch their statuses from BigQuery") {
        for {
          now <- Clock.instant
          jobsRepository <- ZIO.service[JobsRepository]
          bigQuery <- ZIO.service[BigQuery]
          config <- ZIO.service[BigQueryConfig]

          jobService = BigQueryJobServiceLive(
            config,
            jobsRepository,
            bigQuery
          )

          bigQueryJobs <- makeTestBigQueryJobsFromFileName(
            now,
            s"staging/$testTopicName.1.$testDatasetName.$tableName.100.1000.avro"
          )
          _ <- jobService.submitJobs(bigQueryJobs)

          statuses <- jobService.fetchJobsStatuses(
            Fixtures.JobPrefix,
            bigQueryJobs.map(_.creationTime).min
          ).filterOrFail { jobStatuses =>
            val allFinal = bigQueryJobs.forall { job =>
              jobStatuses.get(job.jobId).exists(_.isFinal)
            }
            if (!allFinal) println("Job statuses" + jobStatuses)
            allFinal
          }(new RuntimeException("Not all jobs succeeded"))
            .retry(Schedule.spaced(1.seconds) && Schedule.recurs(10))

          allJobsSucceeded = bigQueryJobs.forall { job =>
            statuses.get(job.jobId).exists(_.isSuccess)
          }

        } yield assertTrue(allJobsSucceeded)
      },
      test("gets BigQuery Jobs with latest Status fetched from BigQuery") {
        for {
          now <- Clock.instant
          jobsRepository <- ZIO.service[JobsRepository]
          bigQuery <- ZIO.service[BigQuery]
          config <- ZIO.service[BigQueryConfig]

          jobService = BigQueryJobServiceLive(
            config,
            jobsRepository,
            bigQuery
          )

          jobsChunk <- makeTestBigQueryJobsFromFileName(
            now,
            s"staging/$testTopicName.1.$testDatasetName.$tableName.100.1000.avro"
          )

          // Submits enrichedBigQueryJobs for processing
          _ <- jobService.submitJobs(jobsChunk)

          // Get Jobs and assert that all jobs are in final state
          jobWithUpdatedStatues <- jobService.jobsWithUpdatedStatus(
            Fixtures.JobPrefix,
            jobsChunk
          ).filterOrFail { jobs =>
            jobs.forall(_.jobStatus.isFinal)
          }(new RuntimeException("Not all jobs succeeded"))
            .retry(Schedule.spaced(1.seconds) && Schedule.recurs(10))

          allJobsSucceeded = jobWithUpdatedStatues.forall(_.jobStatus.isSuccess)
        } yield assertTrue(allJobsSucceeded)
      },
    )
      .provideSomeShared[Scope with Metrics](
        GcsRemoteDirectoryTestConfigLayer,
        BucketTest.layer,
        GcsRemoteDirectory.storageLayer,
        GcsRemoteDirectory.layer,
        GcsJobsRepositoryLive.layer,
        bigQueryLayer,
        bigQueryConfigLayer,
        JobsRepositoryConfig.layer,
      )
      .provideSome[Scope](MetricsTest.layer) @@ withLiveClock @@ ifEnv("RUN_INTEGRATION_TESTS")(
      _ == "true"
    )

  // Test Setup
  private def makeTestBigQueryJobsFromFileName(
      now: Instant,
      fileName: String
  ): ZIO[RemoteDirectory & Scope, Throwable, Chunk[BigQueryJob]] = for {
    remoteDirectory <- ZIO.service[RemoteDirectory]
    out <- remoteDirectory.makeFileFromStream(fileName)
    _ <- ZStream
      .fromResource("europe_topic.0.animals_dataset.cats_table.553.92403.avro")
      .run(ZSink.fromOutputStream(out))
    _ <- ZIO.attemptBlocking(out.close())
    availableAvroFileRefs <- remoteDirectory
      .listFiles
      .filter(_.fileName == fileName)
      .runCollect
  } yield makeTestBigQueryJobs(now, availableAvroFileRefs)

  private def makeTestBigQueryJobs(
      now: Instant,
      availableAvroFileRefs: Chunk[RemoteFileRef]
  ): Chunk[BigQueryJob] =
    Chunk(
      BigQueryJob(
        jobId = Fixtures.jobId(
          testTableId
        ),
        tableId = testTableId,
        fileRefs = availableAvroFileRefs.toVector,
        jobStatus = JobStatus.New,
        creationTime = now,
        retryCount = 0
      )
    )
}
