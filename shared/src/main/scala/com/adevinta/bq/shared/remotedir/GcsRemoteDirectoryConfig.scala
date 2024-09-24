package com.adevinta.bq.shared.remotedir

import com.adevinta.zc.config.Secret
import com.google.auth.Credentials
import com.google.auth.oauth2.GoogleCredentials
import zio._

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets.UTF_8

final case class GcsRemoteDirectoryConfig(
    projectId: Long,
    gcpCredentials: GcpCredentials
)

trait GcpCredentials {
  def credentials: Task[Credentials]
}

object DefaultGcpCredentials extends GcpCredentials {
  override def credentials: Task[Credentials] =
    ZIO.attemptBlocking(GoogleCredentials.getApplicationDefault)
}

final case class ExternalGcpCredentials(serviceAccountKeyJson: Secret) extends GcpCredentials {
  override def credentials: Task[Credentials] =
    ZIO.attemptBlocking {
      val serviceAccountKey = new ByteArrayInputStream(serviceAccountKeyJson.value.getBytes(UTF_8))
      GoogleCredentials.fromStream(serviceAccountKey)
    }
}
