package com.adevinta.bq.shared.remotedir

import java.io.OutputStream
import zio._
import zio.stream.ZStream

import java.time.Instant

case class RemoteFileRef(
    schema: String, // given filepath = gs://bucket-name/path/todir/sub-dir/file.txt, schema = "file" or "gs"
    host: String, // empty for "file", "bucket-name" for "gs"
    prefix: String, // the base path of the remote directory, "path/todir"
    fileName: String, // relative to prefix: "file.txt", but also: "sub-dir/file.txt"
    creationTime: Instant,
) {
  def uri: String = s"$schema://$host/$prefix/$fileName"
}

/** All file names are relative toward the base URI of the remote directory. */
trait RemoteDirectory {
  def list: ZStream[Any, Throwable, String]

  def listFiles: ZStream[Any, Throwable, RemoteFileRef]

  def readFile(fileName: String): ZIO[Any, Throwable, Array[Byte]]

  /** Warning: no provisions are made that closes the stream. */
  def makeFileFromStream(fileName: String): ZIO[Any, Throwable, OutputStream]

  def makeFile(fileName: String, content: Array[Byte]): ZIO[Any, Throwable, Unit]

  def deleteFiles(fileNames: Chunk[String]): ZIO[Any, Throwable, Unit]
}

case class RemoteFileNotFound(fileName: String)
    extends RuntimeException(s"File $fileName not found")
