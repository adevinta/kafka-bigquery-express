package com.adevinta.bq.gcswriter.config

import com.adevinta.bq.shared.remotedir.DefaultGcpCredentials
import com.adevinta.bq.shared.remotedir.GcpProjectIds
import com.adevinta.bq.shared.remotedir.GcsRemoteDirectoryConfig
import com.adevinta.bq.shared.remotedir.RemoteDirectoryConfig
import com.adevinta.zc.config.AppEnvironment
import com.adevinta.zc.config.Secret
import com.adevinta.zc.http.HttpConfig
import zio._

import scala.io.Source

object AppConfigLive {
  def live: TaskLayer[AppConfig] =
    ZLayer(loadConfig)

  private val loadConfig: Task[AppConfig] =
    for {
      appEnvironment <- AppEnvironment.loadFromEnv
      config <- configFor(appEnvironment)
    } yield config

  private def configFor(environment: Option[AppEnvironment]): Task[AppConfig] = {
    val serviceName = "gcs-writer"

    val versionJson = com.adevinta.bigquery.BuildInfo.toJson
    val httpConfig = HttpConfig(8888, serviceName, versionJson)

    environment match {
      case None =>
        // Run this app _without_ the `APP_ENV` environment variable to activate Local Dev Environment
        // with a GCS bucket called test-bucket-[your user name] in the dev project.
        ZIO.logInfo(s"No environment set up, assuming local development environment")
          .as {
            AppConfig(
              httpConfig = httpConfig,
              startOffsetRetrieverConfig = StartOffsetRetrieverConfig(0.seconds),
              kafkaConfig = KafkaConfig(
                NonEmptyChunk("events"),
                List("localhost:9092"),
                None,
                "kafka-copy-dev",
                SchemaRegistryConfig("???"),
              ),
              remoteDirectoryConfig = RemoteDirectoryConfig(
                s"gs://dev-bucket-${sys.env("USER").replace('.', '-')}/test"
              ),
              gcsRemoteDirectoryConfig = GcsRemoteDirectoryConfig(
                projectId = 1234,
                gcpCredentials = DefaultGcpCredentials
              ),
            )
          }
      case Some(AppEnvironment.Dev) =>
        ZIO.fail(new RuntimeException("Dev environment is not supported"))
      case Some(AppEnvironment.Pre) =>
        for {
          kafkaPassword <- getSecret("KAFKA_PASSWORD")
          sourceTopics <- loadSourceTopics("source-topics.txt")
        } yield {
          AppConfig(
            httpConfig = httpConfig,
            startOffsetRetrieverConfig = StartOffsetRetrieverConfig(5.seconds),
            kafkaConfig = KafkaConfig(
              sourceTopics,
              List("kafka-sparklingwood-internal.storage.mpi-internal.com:9094"),
              Some(KafkaCredentials("kafkaUser", kafkaPassword)),
              "gcs-writer-pre",
              SchemaRegistryConfig(
                baseUrl = "https://schema-registry-url.com"
              ),
            ),
            remoteDirectoryConfig = RemoteDirectoryConfig("gs://remote-directory-config/app"),
            gcsRemoteDirectoryConfig = GcsRemoteDirectoryConfig(
              projectId = GcpProjectIds.IntegrationProjectId,
              gcpCredentials = DefaultGcpCredentials
            ),
          )
        }
      case Some(AppEnvironment.Pro) =>
        for {
          kafkaPassword <- getSecret("KAFKA_PASSWORD")
          sourceTopics <- loadSourceTopics("source-topics.txt")
        } yield {
          AppConfig(
            httpConfig = httpConfig,
            startOffsetRetrieverConfig = StartOffsetRetrieverConfig(5.seconds),
            kafkaConfig = KafkaConfig(
              sourceTopics,
              List("a-kafka-broker:9094"),
              Some(KafkaCredentials("kafkaUser", kafkaPassword)),
              "gcs-writer-pro",
              SchemaRegistryConfig(
                baseUrl = "https://schema-registry-url.com"
              ),
            ),
            remoteDirectoryConfig = RemoteDirectoryConfig("gs://remote-directory-config/app"),
            gcsRemoteDirectoryConfig = GcsRemoteDirectoryConfig(
              projectId = GcpProjectIds.ProdProjectId,
              gcpCredentials = DefaultGcpCredentials
            ),
          )
        }
    }
  }

  /** Gets a secret from an environment variable. */
  private def getSecret(name: String): Task[Secret] = {
    ZIO.attempt {
      Secret(sys.env.getOrElse(name, sys.error(s"Missing ENV variable $name")))
    }
  }

  private def loadSourceTopics(resource: String): Task[NonEmptyChunk[String]] = {
    for {
      topics <- ZIO.attempt {
        Chunk.from(
          Source.fromResource(resource)
            .getLines()
            .filter(l => !l.startsWith("#"))
            .map(_.trim)
            .filter(_.nonEmpty)
        )
      }
      result <- ZIO.fromOption(NonEmptyChunk.fromChunk(topics))
        .orElseFail(new RuntimeException(s"File $resource may not be empty"))
    } yield result
  }

}
