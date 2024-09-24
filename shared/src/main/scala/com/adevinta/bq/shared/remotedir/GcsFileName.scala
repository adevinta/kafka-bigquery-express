package com.adevinta.bq.shared.remotedir

import scala.util.matching.Regex

final case class TableId(datasetName: String, tableName: String) {
  override def toString: String = s"$datasetName.$tableName"
}

sealed trait GcsFileName {
  def remoteFileRef: RemoteFileRef
  def topicName: String
  def partition: Int
  def datasetName: String
  def tableName: String
  def schemaId: Int
  def firstOffset: Long

  /** An identifier for files for this segment of this partition. */
  def filePartitionSegmentId: String = s"$topicName.$partition.$datasetName.$tableName.$firstOffset"

  /** An identifier for files for this partition. */
  def filePartitionId: String = s"$topicName.$partition.$datasetName.$tableName"

  /** A unique identifier for the destination table. */
  def tableId: TableId = TableId(datasetName, tableName)
}

final case class AvroGcsFileName(
    remoteFileRef: RemoteFileRef,
    topicName: String,
    partition: Int,
    datasetName: String,
    tableName: String,
    schemaId: Int,
    firstOffset: Long,
) extends GcsFileName

final case class DoneGcsFileName(
    remoteFileRef: RemoteFileRef,
    topicName: String,
    partition: Int,
    datasetName: String,
    tableName: String,
    schemaId: Int,
    firstOffset: Long,
    lastOffset: Long,
) extends GcsFileName

object GcsFileName {

  // $topicName.$partition.$datasetName.$tableName.$schemaId.$firstOffset.avro
  private val gsAvroFileNameRegEx: Regex =
    """staging/([^.]+)\.(\d+)\.([^.]+)\.([^.]+)\.(\d+)\.(\d+)\.avro""".r

  // $topicName.$partition.$datasetName.$tableName.$schemaId.$firstOffset.$lastOffset.done
  private val gsDoneFileNameRegEx: Regex =
    """staging/([^.]+)\.(\d+)\.([^.]+)\.([^.]+)\.(\d+)\.(\d+)\.(\d+)\.done""".r

  def parseFileName(file: RemoteFileRef): Either[FileNameParsingError, GcsFileName] =
    file.fileName match {
      case gsAvroFileNameRegEx(
            topicName,
            partition,
            datasetName,
            tableName,
            schemaId,
            firstOffset
          ) => {
        Right(
          AvroGcsFileName(
            file,
            topicName,
            partition.toInt,
            datasetName,
            tableName,
            schemaId.toInt,
            firstOffset.toLong,
          )
        )
      }
      case gsDoneFileNameRegEx(
            topicName,
            partition,
            datasetName,
            tableName,
            schemaId,
            firstOffset,
            lastOffset
          ) => {
        Right(
          DoneGcsFileName(
            file,
            topicName,
            partition.toInt,
            datasetName,
            tableName,
            schemaId.toInt,
            firstOffset.toLong,
            lastOffset.toLong
          )
        )
      }
      case _ => Left(FileNameParsingError(s"Could not parse $file"))
    }

  final case class FileNameParsingError(message: String) extends Throwable

}
