package com.adevinta.bq.bqwriter

import com.adevinta.bq.bqwriter.config.AppConfig
import com.adevinta.bq.bqwriter.config.AppConfigLive
import com.adevinta.bq.shared.remotedir.GcsRemoteDirectory
import zio._
import zio.logging.consoleJsonLogger
import com.adevinta.zc.http.Http
import com.adevinta.zc.metrics.MetricsLive

object App extends ZIOAppDefault {

  override val bootstrap: ZLayer[Any, Nothing, Unit] =
    Runtime.removeDefaultLoggers >>>
      Runtime.setUnhandledErrorLogLevel(LogLevel.Warning) >>>
      consoleJsonLogger().orDie

  def run: ZIO[Scope, Nothing, ExitCode] = {
    val program =
      logConfig *>
        (Http.runBasicHttpServer <&>
          ZIO.serviceWithZIO[JobCoordinator](_.runLoop()) <&>
          ZIO.serviceWithZIO[StagingRepository](_.runDoneFileCleanup()))

    program
      .provideSome[Scope](
        AppConfigLive.live,
        ZLayer.fromFunction((_: AppConfig).httpConfig),
        ZLayer.fromFunction((_: AppConfig).remoteDirectoryConfig),
        ZLayer.fromFunction((_: AppConfig).jobsRepositoryConfig),
        ZLayer.fromFunction((_: AppConfig).jobCoordinatorConfig),
        ZLayer.fromFunction((_: AppConfig).bqConfig),
        ZLayer.fromFunction((_: AppConfig).gcsRemoteDirectoryConfig),
        MetricsLive.layer,
        GcsRemoteDirectory.storageLayer,
        GcsRemoteDirectory.layer,
        BigQueryJobServiceLive.bigQueryLayer,
        StagingRepositoryLive.layer,
        BigQueryJobServiceLive.layer,
        GcsJobsRepositoryLive.layer,
        JobCoordinatorLive.layer,
      )
      .onError {
        case cause if cause.isInterruptedOnly =>
          ZIO.logWarning(s"Shutting down.")
        case cause => ZIO.logErrorCause(s"Execution failed.", cause)
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
        s"""Starting bq-writer
           |Available processors: $procCount
           |Configuration: $config""".stripMargin
      )
    } yield ()

}
