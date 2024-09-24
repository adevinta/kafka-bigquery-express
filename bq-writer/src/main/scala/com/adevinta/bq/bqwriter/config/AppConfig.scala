package com.adevinta.bq.bqwriter.config

import com.adevinta.bq.shared.remotedir.DefaultGcpCredentials
import com.adevinta.bq.shared.remotedir.GcpCredentials
import com.adevinta.bq.shared.remotedir.GcpProjectIds
import com.adevinta.bq.shared.remotedir.GcsRemoteDirectoryConfig
import com.adevinta.bq.shared.remotedir.RemoteDirectoryConfig
import zio._
import com.adevinta.zc.http.HttpConfig

final case class AppConfig(
    httpConfig: HttpConfig,
    remoteDirectoryConfig: RemoteDirectoryConfig,
    gcsRemoteDirectoryConfig: GcsRemoteDirectoryConfig,
    jobsRepositoryConfig: JobsRepositoryConfig,
    jobCoordinatorConfig: JobCoordinatorConfig,
    bqConfig: BigQueryConfig,
) {

  def projectId: String = gcsRemoteDirectoryConfig.projectId.toString
}

final case class JobsRepositoryConfig(
    // Job definition file name
    jobDefinitionBlobName: String,
)

object JobsRepositoryConfig {
  val default: JobsRepositoryConfig =
    JobsRepositoryConfig(jobDefinitionBlobName = "bq-jobs/jobs.json")
  val layer: ZLayer[Any, Nothing, JobsRepositoryConfig] = ZLayer.succeed(default)
}

final case class JobCoordinatorConfig(
    // Time between scanning for new Avro files and uploading those to Big Query.
    jobSubmissionInterval: Duration,
    // Time between checking the completion of BQ load jobs.
    jobCheckInterval: Duration,
    maxRetries: Int,
    // Fails the entire service when a load job runs longer then the max runtime
    bigQueryJobMaxRuntime: Duration,
    jobPrefix: String,
)

/** Configuration related to connecting to the BigQuery instance at `projectId` */
final case class BigQueryConfig(
    projectId: Long,
    gcpCredentials: GcpCredentials,
    // Might need to be revisited, as we may want to use a different retention policies per table
    partitionRetentionInDays: Int,
    // location, e.g. "europe-west4"
    location: String
) {
  def projectIdString: String = projectId.toString
}

object BigQueryConfig {
  val LocalBigQueryConfig: BigQueryConfig =
    BigQueryConfig(
      projectId = GcpProjectIds.IntegrationProjectId,
      gcpCredentials = DefaultGcpCredentials,
      partitionRetentionInDays = 3,
      location = "europe-west4",
    )
}
