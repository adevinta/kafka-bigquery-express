package com.adevinta.bq.gcswriter.fixtures

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.avro.util.Utf8
import org.apache.avro.specific.SpecificRecordBase

final case class TestRecord(
    var field1: String,
    var field2: Int,
    var eventDateTime: String,
    var requestId: String,
    var userAgent: Option[Utf8],
    var eventId: Utf8,
    var datasetName: Option[Utf8],
    var tableName: Option[Utf8],
    var userId: Option[Utf8],
    var someRecord: SomeRecord,
) extends SpecificRecordBase {

  override def getSchema: Schema = {
    new Schema.Parser().parse("""
        |{
        |  "type": "record",
        |  "name": "TestRecord",
        |  "fields": [
        |    {"name": "field1", "type": "string"},
        |    {"name": "field2", "type": "int"},
        |    {"name": "eventDateTime", "type": "string"},
        |    {"name": "requestId", "type": "string"},
        |    {"name": "userAgent", "type": ["null", "string"], "default": null},
        |    {"name": "eventId", "type": "string"},
        |    {"name": "datasetName", "type": ["null", "string"], "default": null},
        |    {"name": "tableName", "type": ["null", "string"], "default": null},
        |    {"name": "userId", "type": ["null", "string"], "default": null},
        |    {
        |      "name": "someRecord",
        |      "type": {
        |        "type": "record",
        |        "name": "SomeRecord",
        |        "fields": [
        |          {"name": "value", "type": "string"}
        |        ]
        |      }
        |    }
        |  ]
        |}
      """.stripMargin)
  }

  override def get(field: Int): AnyRef = field match {
    case 0 => field1
    case 1 => Int.box(field2)
    case 2 => eventDateTime
    case 3 => requestId
    case 4 => userAgent.orNull
    case 5 => eventId
    case 6 => datasetName.orNull
    case 7 => tableName.orNull
    case 8 => userId.orNull
    case 9 => someRecord
    case _ => throw new IllegalArgumentException(s"Unknown field index: $field")
  }

  override def put(field: Int, value: Any): Unit = field match {
    case 0 => field1 = value.asInstanceOf[String]
    case 1 => field2 = value.asInstanceOf[Int]
    case 2 => eventDateTime = value.asInstanceOf[String]
    case 3 => requestId = value.asInstanceOf[String]
    case 4 => userAgent = Option(value).map(_.asInstanceOf[Utf8])
    case 5 => eventId = value.asInstanceOf[Utf8]
    case 6 => datasetName = Option(value).map(_.asInstanceOf[Utf8])
    case 7 => tableName = Option(value).map(_.asInstanceOf[Utf8])
    case 8 => userId = Option(value).map(_.asInstanceOf[Utf8])
    case 9 => someRecord = value.asInstanceOf[SomeRecord]
    case _ => throw new IllegalArgumentException(s"Unknown field index: $field")
  }
}

object TestRecord {
  def apply(): TestRecord = new TestRecord(
    field1 = "test",
    field2 = 1,
    eventDateTime = "2024-09-13T14:36:45.123+02:00[Europe/Paris]",
    requestId = "requestId",
    userAgent = None,
    eventId = new Utf8("eventId"),
    datasetName = Some(new Utf8("datasetName")),
    tableName = Some(new Utf8("tableName")),
    userId = None,
    someRecord = SomeRecord("defaultValue")
  )
}

// Implementing GenericRecord for SomeRecord
final case class SomeRecord(var value: String) extends GenericRecord {

  override def getSchema: Schema = {
    new Schema.Parser().parse("""
                                |{
                                |  "type": "record",
                                |  "name": "SomeRecord",
                                |  "fields": [
                                |    {"name": "value", "type": "string"}
                                |  ]
                                |}
      """.stripMargin)
  }

  override def get(field: Int): AnyRef = field match {
    case 0 => new Utf8(value)
    case _ => throw new IllegalArgumentException(s"Unknown field index: $field")
  }

  override def put(field: Int, value: Any): Unit = field match {
    case 0 => this.value = value.asInstanceOf[String]
    case _ => throw new IllegalArgumentException(s"Unknown field index: $field")
  }

  override def get(fieldName: String): AnyRef = fieldName match {
    case "value" => new Utf8(value)
    case _       => throw new IllegalArgumentException(s"Unknown field name: $fieldName")
  }

  override def put(fieldName: String, value: Any): Unit = fieldName match {
    case "value" => this.value = value.asInstanceOf[String]
    case _       => throw new IllegalArgumentException(s"Unknown field name: $fieldName")
  }
}
