package com.adevinta.bq.bqwriter.config

import com.adevinta.bq.shared.remotedir.DefaultGcpCredentials
import com.adevinta.bq.shared.remotedir.GcpProjectIds
import com.adevinta.bq.shared.remotedir.GcsRemoteDirectoryConfig
import com.adevinta.bq.shared.remotedir.RemoteDirectoryConfig
import com.adevinta.zc.config.AppEnvironment
import com.adevinta.zc.http.HttpConfig
import zio._

object AppConfigLive {
  def live: TaskLayer[AppConfig] = ZLayer(loadConfig)

  private val loadConfig: Task[AppConfig] =
    for {
      appEnvironment <- AppEnvironment.loadFromEnv
      config <- configFor(appEnvironment)
    } yield config

  private def configFor(environment: Option[AppEnvironment]): Task[AppConfig] = {
    val serviceName = "bq-writer"

    val versionJson = com.adevinta.bigquery.BuildInfo.toJson
    val httpConfig = HttpConfig(8888, serviceName, versionJson)

    environment match {
      case None =>
        // Configured for locally started Kafka (`docker-compose up -d`)
        ZIO.logInfo(s"No environment set up, assuming local development environment")
          .as {
            AppConfig(
              httpConfig,
              RemoteDirectoryConfig(s"gs://some-bucket"),
              GcsRemoteDirectoryConfig(1234, DefaultGcpCredentials),
              JobsRepositoryConfig.default,
              JobCoordinatorConfig(
                jobSubmissionInterval = 30.seconds,
                jobCheckInterval = 30.seconds,
                maxRetries = 2,
                bigQueryJobMaxRuntime = 10.minutes,
                jobPrefix = "integration",
              ),
              bqConfig = BigQueryConfig.LocalBigQueryConfig,
            )
          }
      case Some(AppEnvironment.Dev) =>
        ZIO.fail(new RuntimeException("Dev environment is not supported"))
      case Some(AppEnvironment.Pre) =>
        ZIO.succeed(
          AppConfig(
            httpConfig = httpConfig,
            remoteDirectoryConfig = RemoteDirectoryConfig("gs://some-bucket"),
            gcsRemoteDirectoryConfig = GcsRemoteDirectoryConfig(
              projectId = GcpProjectIds.IntegrationProjectId,
              gcpCredentials = DefaultGcpCredentials,
            ),
            jobsRepositoryConfig = JobsRepositoryConfig.default,
            jobCoordinatorConfig = JobCoordinatorConfig(
              jobSubmissionInterval = 5.minutes,
              jobCheckInterval = 1.minute,
              maxRetries = 1,
              bigQueryJobMaxRuntime = 10.minutes,
              jobPrefix = "integration",
            ),
            bqConfig = BigQueryConfig(
              projectId = GcpProjectIds.IntegrationProjectId,
              gcpCredentials = DefaultGcpCredentials,
              partitionRetentionInDays = 14,
              location = "europe-west4"
            ),
          )
        )
      case Some(AppEnvironment.Pro) =>
        ZIO.succeed(
          AppConfig(
            httpConfig = httpConfig,
            remoteDirectoryConfig = RemoteDirectoryConfig("gs://some-bucket"),
            gcsRemoteDirectoryConfig = GcsRemoteDirectoryConfig(
              projectId = GcpProjectIds.ProdProjectId,
              gcpCredentials = DefaultGcpCredentials,
            ),
            jobsRepositoryConfig = JobsRepositoryConfig.default,
            jobCoordinatorConfig = JobCoordinatorConfig(
              jobSubmissionInterval = 15.minutes,
              jobCheckInterval = 1.minute,
              maxRetries = 3,
              bigQueryJobMaxRuntime = 10.minutes,
              jobPrefix = "integration",
            ),
            bqConfig = BigQueryConfig(
              projectId = GcpProjectIds.ProdProjectId,
              gcpCredentials = DefaultGcpCredentials,
              partitionRetentionInDays = 365,
              location = "europe-west4"
            ),
          )
        )
    }
  }

}
