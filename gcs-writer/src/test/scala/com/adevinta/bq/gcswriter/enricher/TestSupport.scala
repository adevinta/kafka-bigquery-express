package com.adevinta.bq.gcswriter.enricher

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory
import org.apache.avro.util.Utf8
import zio.Chunk

import scala.jdk.CollectionConverters._

object TestSupport {

  def deserializeEvent(
      bytes: Chunk[Byte],
      eventSchema: Schema,
  ): GenericRecord = {
    val reader = new GenericDatumReader[GenericRecord](eventSchema)
    val decoder = DecoderFactory.get().binaryDecoder(bytes.toArray, null)
    reader.read(null, decoder)
  }

  implicit class GenericRecordOps(record: GenericRecord) {
    def getSeqOfString(fieldName: String): Seq[String] = {
      record.get(fieldName).asInstanceOf[GenericData.Array[Utf8]].asScala.map(_.toString).toList
    }

    def getInt(fieldName: String): Int = {
      record.get(fieldName).asInstanceOf[Int]
    }

    def getOptionString(fieldName: String): Option[String] = {
      Option(record.get(fieldName)).map(_.toString)
    }
  }

}
