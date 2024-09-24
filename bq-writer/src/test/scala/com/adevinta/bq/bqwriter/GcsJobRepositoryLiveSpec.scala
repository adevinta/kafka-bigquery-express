package com.adevinta.bq.bqwriter

import com.adevinta.bq.bqwriter.config.JobsRepositoryConfig
import com.adevinta.bq.shared.remotedir.LocalDirectory
import com.adevinta.bq.shared.remotedir.RemoteDirectory
import com.adevinta.bq.shared.remotedir.TempLocalDirectoryConfig
import zio._
import zio.test._

object GcsJobRepositoryLiveSpec extends ZIOSpecDefault {

  override def spec: Spec[Scope & TestEnvironment, Any] =
    suite("GCSJobsRepositoryLive")(
      test(
        "getJobs ensures bucket and status file exists and initializes with empty state if it doesn't"
      ) {
        for {
          jobRepo <- ZIO.service[JobsRepository]
          jobs <- jobRepo.getJobs
        } yield assertTrue(jobs.isEmpty)
      },
      test("getJobs resets to Empty State if the persisted state is not valid") {
        for {
          jobRepo <- ZIO.service[JobsRepository]
          remoteDirectory <- ZIO.service[RemoteDirectory]
          jobConfig <- ZIO.service[JobsRepositoryConfig]
          // Given: current persisted BigQuery Jobs state that is not valid
          _ <- remoteDirectory.makeFile(
            jobConfig.jobDefinitionBlobName,
            """invalid json""".getBytes
          )
          // When: getJobsOrFallback is called with a fallback BigQueryJobs
          result <- jobRepo.getJobs
        } yield {
          // Then: it should reset the persisted state with the fallback state
          assert(result)(Assertion.equalTo(Chunk.empty))
        }
      },
      test("saves and gets the save BigQueryJobs from RemoteDirectory") {
        for {
          jobRepo <- ZIO.service[JobsRepository]
          _ <- jobRepo.saveJobs(Fixtures.testJobs)
          result <- jobRepo.getJobs
        } yield assert(result)(
          Assertion.equalTo(Fixtures.testJobs)
        )
      },
    )
      .provideSome[Scope](
        GcsJobsRepositoryLive.layer,
        JobsRepositoryConfig.layer,
        LocalDirectory.layer,
        TempLocalDirectoryConfig.layer,
      )
}
