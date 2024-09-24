package com.adevinta.bq.gcswriter

import com.adevinta.bq.gcswriter.utils.AvroUtil
import com.adevinta.bq.gcswriter.utils.AvroUtil.ArraySlice
import com.adevinta.bq.gcswriter
import com.adevinta.zc.util.ZioCaching.ZioCachedBy
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8
import zio._

import scala.util.control.NoStackTrace

final case class IncomingKafkaMessage(
    value: Array[Byte],
    headers: Map[String, Utf8],
    offset: Long,
    receivedAt: NanoTime,
    topic: String,
)

final case class ParsedMessage(
    avroBytes: ArraySlice,
    genericRecord: GenericRecord,
    userAgent: Option[String],
    schemaId: SchemaId,
    offset: Long,
    receivedAt: NanoTime,
)

trait EventParser {
  def parse(data: Chunk[IncomingKafkaMessage]): ZIO[Any, Throwable, Chunk[ParsedMessage]]
}

object EventParserLive {
  val layer: ZLayer[SchemaRegistryClient, Nothing, EventParser] = ZLayer {
    for {
      schemaRegistryClient <- ZIO.service[SchemaRegistryClient]
      schemasByIdCache <- Ref.make(Map.empty[Int, Promise[Throwable, Schema]])
    } yield EventParserLive(schemaRegistryClient, schemasByIdCache)
  }

  // Error that really should never be seen.
  private final case class NotAnAvroSchema(schemaId: SchemaId)
      extends RuntimeException(s"Not an Avro schema with id $schemaId")
        with NoStackTrace

  private val UserAgentKafkaHeaderName = "useragenttype"
}

case class EventParserLive(
    schemaRegistryClient: SchemaRegistryClient,
    schemasByIdCache: Ref[Map[Int, Promise[Throwable, Schema]]],
) extends EventParser {
  import EventParserLive._

  override def parse(data: Chunk[IncomingKafkaMessage]): ZIO[Any, Throwable, Chunk[ParsedMessage]] =
    ZIO.foreach(data)(parse)

  private def parse(record: IncomingKafkaMessage): ZIO[Any, Throwable, ParsedMessage] =
    for {
      (schemaId, avroBytes) <- AvroUtil.parseSchemaIdAndAvroBytes(record.value)
      writerSchema <- getSchemaFromRegistry(schemaId)
      genericRecord <- AvroUtil.fromAvroBytes(writerSchema, avroBytes)
        .tapErrorCause(cause =>
          ZIO.logFatalCause(
            s"Failed to parse Avro message with schema id $schemaId, headers: ${record.headers}",
            cause
          )
        )
    } yield {
      val userAgent = record.headers.get(UserAgentKafkaHeaderName).map(_.toString)
      gcswriter.ParsedMessage(
        avroBytes,
        genericRecord,
        userAgent,
        schemaId,
        record.offset,
        record.receivedAt
      )
    }

  private def getSchemaFromRegistry(schemaId: Int): Task[Schema] = {
    ZIO
      .attemptBlocking {
        schemaRegistryClient.getSchemaById(schemaId)
      }
      .flatMap {
        case avroSchema: AvroSchema => ZIO.succeed(avroSchema.rawSchema())
        case _                      => ZIO.fail(NotAnAvroSchema(schemaId))
      }
      .cachedBy(schemasByIdCache, schemaId)
  }

}
