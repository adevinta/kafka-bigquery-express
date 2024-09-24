package com.adevinta.bq.shared.remotedir

import zio.stream.ZStream
import zio._

import java.io.FileOutputStream
import java.io.IOException
import java.io.OutputStream
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file._

object LocalDirectory {
  def layer: ZLayer[RemoteDirectoryConfig, Throwable, RemoteDirectory] = ZLayer {
    for {
      config <- ZIO.service[RemoteDirectoryConfig]
      layer <-
        if (config.baseUri.startsWith("file:///")) {
          val basePath = Path.of(config.baseUri.stripPrefix("file://"))
          ZIO.attemptBlocking(Files.isDirectory(basePath))
            .filterOrFail(identity)(new IllegalArgumentException(s"'$basePath' is not a directory"))
            .as(LocalDirectory(basePath))
        } else {
          ZIO.fail(
            new IllegalArgumentException(
              s"Invalid basePath: ${config.baseUri}, it must start with file:///"
            )
          )
        }
    } yield layer
  }
}

final case class LocalDirectory(basePath: Path) extends RemoteDirectory {

  override def list: ZStream[Any, Throwable, String] = listFiles.map(_.fileName)

  override def listFiles: ZStream[Any, Throwable, RemoteFileRef] =
    ZStream.fromIterableZIO {
      ZIO.attemptBlocking {
        val removeFileRefs = Chunk.newBuilder[RemoteFileRef]
        Files.walkFileTree(
          basePath,
          new SimpleFileVisitor[Path] {
            override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
              val remoteFileRef = RemoteFileRef(
                schema = "file",
                host = "",
                prefix = basePath.toString.stripPrefix("/"),
                fileName = basePath.relativize(file).toString,
                creationTime = attrs.creationTime().toInstant,
              )
              removeFileRefs += remoteFileRef
              FileVisitResult.CONTINUE
            }

            override def visitFileFailed(file: Path, exc: IOException): FileVisitResult =
              exc match {
                case _: NoSuchFileException => FileVisitResult.CONTINUE
                case _                      => throw exc
              }
          }
        )
        removeFileRefs.result()
      }
    }

  override def readFile(fileName: String): ZIO[Any, Throwable, Array[Byte]] = {
    val f = basePath.resolve(fileName)
    ZIO.attemptBlocking(Files.readAllBytes(f))
      .catchSome { case ex: NoSuchFileException =>
        ZIO.fail(RemoteFileNotFound(fileName))
      }
  }

  override def makeFileFromStream(fileName: String): ZIO[Any, Throwable, OutputStream] = {
    val f = basePath.resolve(fileName)
    ZIO.attemptBlocking(Files.createDirectories(f.getParent)) *>
      ZIO.attemptBlocking(new FileOutputStream(f.toFile))
  }

  override def makeFile(fileName: String, content: Array[Byte]): ZIO[Any, Throwable, Unit] = {
    val f = basePath.resolve(fileName)
    ZIO.attemptBlocking(Files.createDirectories(f.getParent)) *>
      ZIO.attemptBlocking(Files.write(f, content)).unit
  }

  override def deleteFiles(fileNames: Chunk[String]): ZIO[Any, Throwable, Unit] =
    ZIO.foreachDiscard(fileNames) { filename =>
      ZIO.attemptBlocking(Files.deleteIfExists(basePath.resolve(filename)))
    }.unit
}
