package com.adevinta.bq.gcswriter

import com.adevinta.bq.gcswriter.config.AppConfig
import com.adevinta.bq.gcswriter.config.AppConfigLive
import com.adevinta.bq.gcswriter.enricher.EventEnricherLive
import com.adevinta.bq.shared.remotedir.GcsRemoteDirectory
import com.adevinta.zc.http.Http
import com.adevinta.zc.metrics.MetricsLive
import zio._
import zio.logging.consoleJsonLogger

/** Run this app _without_ the `APP_ENV` environment variable to connect to a locally running Kafka.
  * A kafka can be started with docker: `docker-compose up -d`.
  */
object App extends ZIOAppDefault {

  override val bootstrap: ZLayer[Any, Nothing, Unit] =
    Runtime.removeDefaultLoggers >>>
      Runtime.setUnhandledErrorLogLevel(LogLevel.Warning) >>>
      consoleJsonLogger().orDie

  def run: ZIO[Scope, Nothing, ExitCode] = {
    val program =
      logConfig *> (
        Http.runBasicHttpServer <&>
          ZIO.serviceWithZIO[MessageProcessor](_.run())
      )

    program
      .provideSome[Scope](
        AppConfigLive.live,
        ZLayer.fromFunction((_: AppConfig).httpConfig),
        ZLayer.fromFunction((_: AppConfig).startOffsetRetrieverConfig),
        ZLayer.fromFunction((_: AppConfig).kafkaConfig),
        ZLayer.fromFunction((_: AppConfig).remoteDirectoryConfig),
        ZLayer.fromFunction((_: AppConfig).gcsRemoteDirectoryConfig),
        MetricsLive.layer,
        SchemaRegistry.layer,
        StartOffsetRetrieverLive.layer,
        EventParserLive.layer,
        EventEnricherLive.layer,
        GcsRemoteDirectory.storageLayer,
        GcsRemoteDirectory.layer,
        AvroFileWriterLive.layer,
        MessageProcessorLive.layer
      )
      .onError {
        case cause if cause.isInterruptedOnly => ZIO.logWarning(s"Shutting down...")
        case cause                            => ZIO.logErrorCause(s"Execution failed", cause)
      }
      .exitCode <*
      // Give logback some time to actually log before the JVM exits.
      ZIO.sleep(100.millis)
  }

  private val logConfig =
    for {
      config <- ZIO.service[AppConfig]
      procCount = java.lang.Runtime.getRuntime.availableProcessors()
      _ <- ZIO.logWarning(
        s"""Starting gcs-writer
           |Available processors: $procCount
           |Configuration: $config""".stripMargin
      )
    } yield ()

}
