package com.adevinta.bq.gcswriter.enricher

import com.adevinta.bq.gcswriter.ParsedMessage
import com.adevinta.bq.gcswriter.utils.AvroUtil
import com.adevinta.bq.gcswriter._
import EventEnricherLive.MissingFieldException
import AvroUtil.recordToBytes
import zio._

import java.time.ZonedDateTime
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord

import scala.util.control.NoStackTrace

/** @param schemaId
  *   schemaId obtained during parsing operation
  * @param extendedAvroBytes
  *   Output of the enriching operation: avro bytes containing the extra fields prepended to the
  *   original avro bytes
  * @param extendedSchema
  *   Avro schema containing the extra fields prepended to the original schema
  * @param datasetName
  *   The name of the dataset this message should be written to
  * @param tableName
  *   The name of the table this message should be written to
  * @param offset
  *   kafka offset
  * @param receivedAt
  *   in Nano
  */
final case class EnrichedMessage(
    schemaId: SchemaId,
    extendedAvroBytes: Chunk[Byte],
    extendedSchema: Schema,
    datasetName: String,
    tableName: String,
    offset: Long,
    receivedAt: NanoTime,
)
trait EventEnricher {
  def enrich(data: Chunk[ParsedMessage]): ZIO[Any, Throwable, Chunk[EnrichedMessage]]
}

object EventEnricherLive {

  def layer: ZLayer[Any, Throwable, EventEnricher] =
    ZLayer.succeed {
      EventEnricherLive()
    }

  final case class MissingFieldException(msg: String) extends Throwable(msg) with NoStackTrace
}

final case class EventEnricherLive(
) extends EventEnricher {

  private val schemaExtender = new SchemaExtender

  override def enrich(data: Chunk[ParsedMessage]): ZIO[Any, Throwable, Chunk[EnrichedMessage]] =
    ZIO.foreach(data)(enrich)

  /** @param parsedMessage
    * @return
    *   the message enriched with the extra fields This function contains basic logic to show the
    *   behaviour
    */
  private def enrich(parsedMessage: ParsedMessage): ZIO[Any, Throwable, EnrichedMessage] =
    for {
      // Extract values to initialize in the fields of the new schema
      eventDateTime <- getStringField(
        parsedMessage.genericRecord,
        List("eventDateTime")
      ).map {
        ZonedDateTime.parse(_)
          .toLocalDate.toEpochDay.toInt
      }

      // Instantiate new part of the schema
      extraFieldsRecord = {
        val record = new GenericData.Record(SchemaExtender.additionalFieldsSchema)
        record.put(0, eventDateTime)
        record
      }
      // Extend the existing schema
      extendedSchema = schemaExtender.extendWithAdditionalFields(
        parsedMessage.genericRecord.getSchema,
        parsedMessage.schemaId,
      )

      // Prepending the avro bytes results in a valid Avro record according to the extended schema
      extraFieldBytes <- recordToBytes(extraFieldsRecord)
      extendedAvroBytes = extraFieldBytes ++ parsedMessage.avroBytes.asChunk

      // Extract destination dataset / table
      // Here we pull the destination from the message content.
      // Other possible sources are kafka record headers (e.g. like `parsedMessage.userAgent`), or from configuration.
      datasetName <- getStringField(parsedMessage.genericRecord, List("datasetName"))
      tableName <- getStringField(parsedMessage.genericRecord, List("tableName"))

    } yield EnrichedMessage(
      schemaId = parsedMessage.schemaId,
      extendedAvroBytes = extendedAvroBytes,
      extendedSchema = extendedSchema,
      datasetName = datasetName,
      tableName = tableName,
      offset = parsedMessage.offset,
      receivedAt = parsedMessage.receivedAt
    )

  private def getStringField(record: GenericRecord, fieldName: List[String]): Task[String] =
    AvroUtil.stringField(record, fieldName).flatMap {
      case Some(value) => ZIO.succeed(value)
      case None => ZIO.fail(MissingFieldException(s"Field ${fieldName.mkString(".")} not found"))
    }

}
