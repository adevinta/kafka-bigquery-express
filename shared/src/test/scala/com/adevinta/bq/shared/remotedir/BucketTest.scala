package com.adevinta.bq.shared.remotedir

import com.google.cloud.storage.Storage
import zio._

import java.util.UUID

object BucketTest {

  private def generateBucketName(): ZIO[Any, Nothing, String] =
    ZIO.succeed(s"bigquery-testbucket-" + UUID.randomUUID())

  val layer: ZLayer[Scope & GcsRemoteDirectoryConfig & Storage, Throwable, RemoteDirectoryConfig] =
    ZLayer.succeed {
      RemoteDirectoryConfig("gs://" + generateBucketName() + "/test-data")
    }
}
