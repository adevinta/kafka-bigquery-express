package com.adevinta.bq.bqwriter.mocks

import com.adevinta.bq.bqwriter.BigQueryJobService
import com.adevinta.bq.bqwriter.job.BigQueryJob
import zio._
import zio.mock.Mock
import zio.mock.Proxy

object BigQueryJobServiceMock extends Mock[BigQueryJobService] {
  object RunningJobsWithUpdatedStatus
      extends Effect[(String, Chunk[BigQueryJob]), Throwable, Chunk[BigQueryJob]]
  object SubmitJobs extends Effect[Chunk[BigQueryJob], Throwable, Unit]

  val compose: URLayer[Proxy, BigQueryJobService] = ZLayer {
    ZIO.serviceWith[Proxy] { proxy =>
      new BigQueryJobService {
        override def jobsWithUpdatedStatus(
            jobIdPrefix: String,
            runningJobs: Chunk[BigQueryJob]
        ): Task[Chunk[BigQueryJob]] =
          proxy(RunningJobsWithUpdatedStatus, jobIdPrefix, runningJobs)

        override def submitJobs(newJobs: Chunk[BigQueryJob]): ZIO[Any, Throwable, Unit] =
          proxy(SubmitJobs, newJobs)
      }
    }
  }
}
