package com.adevinta.bq.bqwriter.mocks

import com.adevinta.bq.bqwriter.JobsRepository
import com.adevinta.bq.bqwriter.job.BigQueryJob
import zio._
import zio.mock.Mock
import zio.mock.Proxy

object JobsRepositoryMock extends Mock[JobsRepository] {
  object SaveJobs extends Effect[Chunk[BigQueryJob], Throwable, Unit]
  object GetJobs extends Effect[Unit, Throwable, Chunk[BigQueryJob]]

  val compose: URLayer[Proxy, JobsRepository] = ZLayer {
    ZIO.serviceWith[Proxy] { proxy =>
      new JobsRepository {
        override def saveJobs(jobs: Chunk[BigQueryJob]): Task[Unit] =
          proxy(SaveJobs, jobs)

        override def getJobs: Task[Chunk[BigQueryJob]] =
          proxy(GetJobs)
      }
    }
  }
}
