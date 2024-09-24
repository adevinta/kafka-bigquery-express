package com.adevinta.bq.shared.remotedir

import java.io.OutputStream
import java.nio.channels.Channels

import com.google.cloud.storage._
import zio._
import zio.stream.ZStream

object GcsRemoteDirectory {
  private val gsUrlRegEx = """gs://([^/]+)/(.*)""".r

  def storageLayer: ZLayer[GcsRemoteDirectoryConfig, Throwable, Storage] = ZLayer {
    for {
      gcsRemoteConfig <- ZIO.service[GcsRemoteDirectoryConfig]
      credentials <- gcsRemoteConfig.gcpCredentials.credentials
      storage <- ZIO.attemptBlocking {
        StorageOptions
          .newBuilder()
          .setProjectId(gcsRemoteConfig.projectId.toString)
          .setCredentials(credentials)
          .build()
          .getService
      }
    } yield storage
  }

  def layer: ZLayer[RemoteDirectoryConfig & Storage, Throwable, RemoteDirectory] = ZLayer {
    for {
      config <- ZIO.service[RemoteDirectoryConfig]
      storage <- ZIO.service[Storage]
      (bucket, pathPrefix) <- parseGsBaseDir(config.baseUri)
    } yield GcsRemoteDirectory(bucket, pathPrefix, storage)
  }

  private def parseGsBaseDir(baseDir: String): ZIO[Any, Throwable, (String, String)] = {
    baseDir match {
      case gsUrlRegEx(bucket, path) => ZIO.succeed((bucket, path.stripSuffix("/")))
      case _ => ZIO.fail(new IllegalArgumentException(s"Invalid basePath: $baseDir"))
    }
  }
}

final case class GcsRemoteDirectory(
    bucket: String,
    pathPrefix: String,
    storage: Storage,
) extends RemoteDirectory {
  private val prefixWithSlash = pathPrefix + "/"

  override def list: ZStream[Any, Throwable, String] = listFiles.map(_.fileName)

  override def listFiles: ZStream[Any, Throwable, RemoteFileRef] = ZStream.unwrap {
    ZIO.attemptBlocking {
      ZStream
        .fromJavaStream(
          storage.list(bucket, Storage.BlobListOption.prefix(pathPrefix)).streamAll()
        )
        .map { blob =>
          RemoteFileRef(
            schema = "gs",
            host = bucket,
            prefix = pathPrefix,
            fileName = blob.getName.stripPrefix(prefixWithSlash),
            creationTime = blob.getCreateTimeOffsetDateTime.toInstant,
          )
        }
    }
  }

  override def readFile(fileName: String): ZIO[Any, Throwable, Array[Byte]] = ZIO.attemptBlocking {
    val blobId = BlobId.of(bucket, s"$prefixWithSlash$fileName")
    storage.readAllBytes(blobId)
  }
    .catchSome {
      case ex: StorageException if ex.getCode == 404 => ZIO.fail(RemoteFileNotFound(fileName))
    }

  override def makeFileFromStream(fileName: String): ZIO[Any, Throwable, OutputStream] =
    ZIO.attemptBlocking {
      val blobId = BlobId.of(bucket, s"$prefixWithSlash$fileName")
      val blobInfo = BlobInfo.newBuilder(blobId).build()
      val writeChannel = storage.writer(blobInfo, Storage.BlobWriteOption.disableGzipContent())
      // If the Chunk Size is not specified, by default it is set to`16MB`.
      // Hence, we are setting it to min allowed chunk size, which is 256 KB.
      writeChannel.setChunkSize(256 * 1024)
      Channels.newOutputStream(writeChannel)
    }

  override def makeFile(fileName: String, content: Array[Byte]): ZIO[Any, Throwable, Unit] =
    ZIO
      .attemptBlocking {
        import com.google.cloud.storage.Storage.BlobTargetOption.detectContentType
        val blobId = BlobId.of(bucket, s"$prefixWithSlash$fileName")
        val blobInfo = BlobInfo.newBuilder(blobId).build()
        storage.create(blobInfo, content, detectContentType())
      }
      .retry(Schedule.fibonacci(100.millis) && Schedule.recurs(2))
      .unit

  override def deleteFiles(fileNames: Chunk[String]): ZIO[Any, Throwable, Unit] = {
    val deleteBatchSize = 500
    ZStream
      .fromIterator(fileNames.grouped(deleteBatchSize))
      .mapZIO { fileNameChunk =>
        ZIO
          .attemptBlocking {
            storage.delete(
              fileNameChunk.map(fileName => BlobId.of(bucket, s"$prefixWithSlash$fileName")): _*
            )
          }.tapErrorCause { cause =>
            ZIO.logError(s"Failed to delete files: ${cause.prettyPrint}")
          }
          .retry(Schedule.fibonacci(100.millis) && Schedule.recurs(2))
      }
      .runDrain
  }
}
