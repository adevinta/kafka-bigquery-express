package com.adevinta.bq.shared.remotedir

import zio._
import zio.test._

object LocalDirectorySpec extends ZIOSpecDefault {

  override def spec: Spec[TestEnvironment & Scope, Any] = {
    RemoteDirectorySuite.remoteDirectorySuite("LocalDirectorySpec")
      .provideSomeShared[Scope](
        TempLocalDirectoryConfig.layer,
        LocalDirectory.layer,
      )
  }

}
