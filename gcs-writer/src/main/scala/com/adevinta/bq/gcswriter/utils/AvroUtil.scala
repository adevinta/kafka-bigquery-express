package com.adevinta.bq.gcswriter.utils

import com.adevinta.bq.gcswriter.SchemaId
import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory
import zio._

import java.nio.ByteBuffer
import scala.annotation.tailrec
import scala.util.control.NoStackTrace
import java.io.ByteArrayOutputStream
import scala.util.Using
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.EncoderFactory

object AvroUtil {
  private case object NotAConfluentAvroMessage
      extends RuntimeException(
        "Kafka message did not start with magic byte '0' and 4 byte schema id"
      )
        with NoStackTrace

  final case class ArraySlice(bytes: Array[Byte], offset: Int, length: Int) {
    require(offset >= 0 && length >= 0 && offset + length <= bytes.length)

    def asChunk: Chunk[Byte] = Chunk.fromArray(bytes).slice(offset, offset + length)
  }

  object ArraySlice {
    def apply(bytes: Array[Byte]): ArraySlice = ArraySlice(bytes, 0, bytes.length)
  }

  /** The wire format consists of a fixed magic byte, the schema ID and the avro bytes. See:
    * https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#wire-format
    */
  def parseSchemaIdAndAvroBytes(rawMessage: Array[Byte]): Task[(SchemaId, ArraySlice)] = {
    val MagicByte: Byte = 0x0
    ZIO.attempt {
      val buffer = ByteBuffer.wrap(rawMessage)
      val magic = buffer.get
      val schemaId = buffer.getInt
      (magic, schemaId)
    }
      .orElseFail(NotAConfluentAvroMessage)
      .flatMap {
        case (MagicByte, schemaId) if schemaId >= 0 =>
          ZIO.attempt {
            val skipBytes = java.lang.Byte.BYTES + java.lang.Integer.BYTES
            val avroBytes = ArraySlice(rawMessage, skipBytes, rawMessage.length - skipBytes)
            (schemaId, avroBytes)
          }
            .orElseFail(NotAConfluentAvroMessage)
        case _ =>
          ZIO.fail(NotAConfluentAvroMessage)
      }
  }

  /** Deserializes Avro bytes. */
  def fromAvroBytes(writerSchema: Schema, avroBytes: ArraySlice): Task[GenericRecord] =
    ZIO.attempt {
      val decoder = DecoderFactory
        .get()
        .binaryDecoder(avroBytes.bytes, avroBytes.offset, avroBytes.length, null)
      val avroReader = new GenericDatumReader[GenericRecord](writerSchema)
      avroReader.read(null, decoder)
    }

  /** Extracts a field from a generic record. */
  def stringField(record: GenericRecord, fieldPath: List[String]): Task[Option[String]] = {
    ZIO.attempt(stringFieldUnsafe(record, fieldPath))
  }

  def stringFieldUnsafe(record: GenericRecord, fieldPath: List[String]): Option[String] = {
    @tailrec
    def loop(record: GenericRecord, fieldPath: List[String]): Option[String] = {
      record.get(fieldPath.head) match {
        case null                                   => Option.empty[String]
        case c: CharSequence if fieldPath.size == 1 => Some(c.toString)
        case g: GenericRecord if fieldPath.size > 1 => loop(g, fieldPath.tail)
        case _                                      => Option.empty[String]
      }
    }
    loop(record, fieldPath)
  }

  /** Serializes GenericRecord in Avro bytes. */
  def recordToBytes(genericRecord: GenericRecord): Task[Chunk[Byte]] = ZIO.attempt {
    val datumWriter = new GenericDatumWriter[GenericRecord](genericRecord.getSchema)
    Using.resource(new ByteArrayOutputStream()) { bos =>
      val enc = EncoderFactory.get().directBinaryEncoder(bos, null)
      datumWriter.write(genericRecord, enc)
      bos.flush()
      Chunk.fromArray(bos.toByteArray)
    }
  }

}
