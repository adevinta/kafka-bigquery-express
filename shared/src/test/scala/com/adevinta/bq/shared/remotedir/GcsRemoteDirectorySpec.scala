package com.adevinta.bq.shared.remotedir

import zio._
import zio.test.TestAspect.ifEnv
import zio.test._

object GcsRemoteDirectorySpec extends ZIOSpecDefault {

  private val TestConfig: GcsRemoteDirectoryConfig =
    GcsRemoteDirectoryConfig(GcpProjectIds.IntegrationProjectId, DefaultGcpCredentials)

  private val TestConfigLayer: ZLayer[Any, Nothing, GcsRemoteDirectoryConfig] =
    ZLayer.succeed(TestConfig)

  override def spec: Spec[TestEnvironment & Scope, Any] = {
    RemoteDirectorySuite
      .remoteDirectorySuite("GcsRemoteDirectorySpec")
      .provideSomeShared[Scope](
        TestConfigLayer,
        GcsRemoteDirectory.storageLayer,
        BucketTest.layer,
        GcsRemoteDirectory.layer,
      ) @@ ifEnv("RUN_INTEGRATION_TESTS")(_ == "true")
  }

}
