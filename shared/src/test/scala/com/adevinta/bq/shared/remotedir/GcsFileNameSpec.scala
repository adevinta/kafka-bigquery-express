package com.adevinta.bq.shared.remotedir

import zio._
import zio.test.Spec
import zio.test.TestEnvironment
import zio.test.ZIOSpecDefault
import zio.test.assertTrue

import java.time.Instant
import scala.util.chaining._
import scala.language.implicitConversions

object GcsFileNameSpec extends ZIOSpecDefault {

  private val topic = "topic_name"
  private val partition = 10
  private val datasetName = "animals"
  private val tableName = "cats_table"
  private val schemaId = 100
  private val firstOffset = 1
  private val lastOffset = 52L
  private val filePartitionSegmentId = s"$topic.$partition.$datasetName.$tableName.$firstOffset"
  private val filePartitionId = s"$topic.$partition.$datasetName.$tableName"

  override def spec: Spec[TestEnvironment & Scope, Any] = {
    suite("GcsFileNameSpec")(
      suite("parses valid file names")(
        validFileNames.map { fileName =>
          test(s"should parse valid filename '$fileName'") {
            val parsedGcsFileName = GcsFileName.parseFileName(makeRemoteFileRef(fileName))
            assertTrue(parsedGcsFileName.isRight)
          }
        }: _*,
      ),
      suite("GcsFileNameSpec specific test")(
        test("should parse a valid avro file name") {
          val filename =
            s"staging/$topic.$partition.$datasetName.$tableName.$schemaId.$firstOffset.avro"
          val remoteFileRef = makeRemoteFileRef(filename)
          val result = GcsFileName.parseFileName(remoteFileRef)
          assertTrue(
            result.isRight,
            isRight(result)(_.isInstanceOf[AvroGcsFileName]),
            isRight(result)(_.remoteFileRef == remoteFileRef),
            isRight(result)(_.topicName == topic),
            isRight(result)(_.partition == partition),
            isRight(result)(_.datasetName == datasetName),
            isRight(result)(_.tableName == tableName),
            isRight(result)(_.schemaId == schemaId),
            isRight(result)(_.firstOffset == firstOffset),
            isRight(result)(_.filePartitionSegmentId == filePartitionSegmentId),
            isRight(result)(_.filePartitionId == filePartitionId),
            isRight(result)(
              _.tableId == TableId(datasetName, tableName)
            ),
          )

        },
        test("should parse a valid done file name") {
          val filename =
            s"staging/$topic.$partition.$datasetName.$tableName.$schemaId.$firstOffset.$lastOffset.done"
          val remoteFileRef = makeRemoteFileRef(filename)
          val result = GcsFileName.parseFileName(remoteFileRef)
          assertTrue(
            result.isRight,
            isRight(result)(_.isInstanceOf[DoneGcsFileName]),
            isRight(result)(_.remoteFileRef == remoteFileRef),
            isRight(result)(_.topicName == topic),
            isRight(result)(_.partition == partition),
            isRight(result)(_.datasetName == datasetName),
            isRight(result)(_.tableName == tableName),
            isRight(result)(_.schemaId == schemaId),
            isRight(result)(_.firstOffset == firstOffset),
            isRight(result)(_.asInstanceOf[DoneGcsFileName].lastOffset == lastOffset),
            isRight(result)(_.filePartitionSegmentId == filePartitionSegmentId),
            isRight(result)(_.filePartitionId == filePartitionId),
            isRight(result)(
              _.tableId == TableId(datasetName, tableName)
            ),
          )
        },
        test(
          "should fail parsing a string that doesn't correspond to a file name for a done file or an avro file"
        ) {
          def invalidFileName(fileName: String): Boolean =
            GcsFileName.parseFileName(makeRemoteFileRef("staging/" + fileName)).isLeft
          assertTrue(
            invalidFileName(
              s"$topic.PARTITION.$datasetName.$tableName.$schemaId.$firstOffset.$lastOffset.done"
            ),
            invalidFileName(
              s"$topic.$partition.$datasetName.$tableName.SCHEMAID.$firstOffset.$lastOffset.done"
            ),
            invalidFileName(
              s"$topic.$partition.$datasetName.$tableName.$schemaId.FIRSTOFFSET.$lastOffset.done"
            ),
            invalidFileName(
              s"$topic.$partition.$datasetName.$tableName.$schemaId.$firstOffset.LASTOFFSET.done"
            ),
            invalidFileName(
              s"$topic.$partition.$datasetName.$tableName.$schemaId.$firstOffset.$lastOffset.ANOTHER"
            ),
            invalidFileName(
              s"$topic.$partition.$datasetName.$tableName.SCHEMAID.$firstOffset.avro"
            ),
            invalidFileName(
              s"$topic.$partition.$datasetName.$tableName.$schemaId.FISRTOFFSET.avro"
            ),
            invalidFileName(
              s"$topic.$partition.$datasetName.$tableName.$schemaId.$firstOffset.LASTOFFSET.avro"
            ),
            invalidFileName(
              s"$topic.$partition.$datasetName.$tableName.$schemaId.$firstOffset.ANOTHER"
            )
          )
        },
      )
    )
  }

  private def makeRemoteFileRef(filename: String): RemoteFileRef =
    RemoteFileRef("file", "", "", filename, Instant.EPOCH)

  private def isRight(value: Either[_, GcsFileName])(f: GcsFileName => Boolean): Boolean =
    value.fold(_ => false, f)

  private val validFileNames: Chunk[String] =
    """
      |topic_name.1.animals.cats_table.2.1001.avro
      |topic_name.1.animals.cats_table.2.1001.2001.done
      |topic_name.1.animals.dogs_table.2.1001.avro
      |another_topic_name.1.animals.dogs.2.1001.2001.done
      |another_topic_name.100.animals.dogs.2.9999.100010.done
      |""".stripMargin
      .split("\n")
      .map(_.trim)
      .filter(_.nonEmpty)
      .map("staging/" + _)
      .pipe(Chunk.fromIterable(_))

}
