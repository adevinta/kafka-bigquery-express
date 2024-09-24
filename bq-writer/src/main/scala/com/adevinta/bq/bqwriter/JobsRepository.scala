package com.adevinta.bq.bqwriter

import com.adevinta.bq.bqwriter.AppError.JsonDecodingError
import com.adevinta.bq.bqwriter.config.JobsRepositoryConfig
import com.adevinta.bq.bqwriter.job.BigQueryJob
import com.adevinta.bq.bqwriter.job.JobsSerde
import zio._
import com.adevinta.bq.shared.remotedir.RemoteDirectory
import com.adevinta.bq.shared.remotedir.RemoteFileNotFound
import com.github.plokhotnyuk.jsoniter_scala.core.JsonValueCodec
import com.github.plokhotnyuk.jsoniter_scala.macros.CodecMakerConfig
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker

/** An abstraction that handles the persistence of [[BigQueryJob]]s. It provides methods to save and
  * get [[BigQueryJob]]s from a remote storage, with a fallback mechanism for loading jobs.
  */
trait JobsRepository {

  def saveJobs(jobs: Chunk[BigQueryJob]): Task[Unit]

  def getJobs: Task[Chunk[BigQueryJob]]
}

object GcsJobsRepositoryLive {
  val layer: ZLayer[JobsRepositoryConfig with RemoteDirectory, Nothing, JobsRepository] = ZLayer {
    for {
      remoteDirectory <- ZIO.service[RemoteDirectory]
      jobRepositoryConfig <- ZIO.service[JobsRepositoryConfig]
    } yield {
      GcsJobsRepositoryLive(
        remoteDirectory,
        jobRepositoryConfig
      )
    }
  }

  /** All running BigQuery load jobs as stored in the state file on GCS. */
  final case class BigQueryJobsState(jobList: Vector[BigQueryJob]) {
    def size: Int = jobList.size
  }

  object BigQueryJobsState {
    val empty: BigQueryJobsState = BigQueryJobsState(Vector.empty)

    implicit val bqLoadJobsStateCodec: JsonValueCodec[BigQueryJobsState] =
      JsonCodecMaker.make(CodecMakerConfig)
  }

}

final case class GcsJobsRepositoryLive(
    remoteDirectory: RemoteDirectory,
    jobRepositoryConfig: JobsRepositoryConfig,
) extends JobsRepository {
  import GcsJobsRepositoryLive._

  private val jobDefinitionBlobName = jobRepositoryConfig.jobDefinitionBlobName

  override def saveJobs(jobs: Chunk[BigQueryJob]): Task[Unit] = {
    JobsSerde
      .serialize(BigQueryJobsState(jobs.toVector))
      .flatMap(remoteDirectory.makeFile(jobDefinitionBlobName, _))
  }

  override def getJobs: UIO[Chunk[BigQueryJob]] = {
    val fallbackDefinition = Chunk.empty[BigQueryJob]
    remoteDirectory
      .readFile(jobDefinitionBlobName)
      .flatMap(JobsSerde.deserialize)
      .map(jobs => Chunk.fromIterable(jobs.jobList))
      .catchSome { case _: RemoteFileNotFound | _: JsonDecodingError =>
        for {
          _ <- ZIO.logError(
            s"File $jobDefinitionBlobName does not exist or contains invalid json." +
              " Fallback to empty job state."
          )
          _ <- saveJobs(fallbackDefinition)
        } yield fallbackDefinition
      }
      .logError
      .orDie
  }

}
