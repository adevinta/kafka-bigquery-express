package com.adevinta.bq.shared.remotedir

import zio._

import java.io.IOException
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.FileVisitResult
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.SimpleFileVisitor

object TempLocalDirectoryConfig {

  /** Provides a `RemoteDirectoryConfig` pointing to a temporary directory on the local file system.
    */
  val layer: ZLayer[Scope, Throwable, RemoteDirectoryConfig] =
    ZLayer {
      for {
        tmpDir <- ZIO.acquireRelease {
          ZIO.attemptBlocking(Files.createTempDirectory("bigquery-test"))
            .tap(tmpDir => ZIO.logInfo(s"Created temp directory $tmpDir"))
        } { tmpDir: Path =>
          ZIO.logInfo(s"Deleting temp directory $tmpDir") *> deleteDirRecursively(tmpDir).ignore
        }
      } yield RemoteDirectoryConfig("file://" + tmpDir.toAbsolutePath.toString)
    }

  private def deleteDirRecursively(dir: Path): Task[Unit] = ZIO.attemptBlocking {
    Files.walkFileTree(
      dir,
      new SimpleFileVisitor[Path]() {
        override def visitFile(file: Path, attr: BasicFileAttributes): FileVisitResult = {
          Files.delete(file)
          FileVisitResult.CONTINUE
        }

        override def postVisitDirectory(dir: Path, exc: IOException): FileVisitResult = {
          Files.delete(dir)
          FileVisitResult.CONTINUE
        }
      }
    )
  }
}
