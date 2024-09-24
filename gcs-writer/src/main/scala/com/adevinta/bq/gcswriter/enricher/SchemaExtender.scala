package com.adevinta.bq.gcswriter.enricher

import com.adevinta.bq.gcswriter.SchemaId
import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._
import org.apache.avro.JsonProperties
import org.apache.avro.LogicalTypes
import org.apache.avro.Schema

object SchemaExtender {
  private[enricher] val EventDateFieldName = "eventDate"

  private[enricher] val additionalFieldsSchema: Schema = {
    val extraField = new Schema.Field(
      EventDateFieldName,
      LogicalTypes.date.addToSchema(Schema.create(Schema.Type.INT)),
      "Event date extracted from event.eventDateTime to partition the data in BigQuery",
    )
    val extraFields = List(extraField)
    Schema.createRecord(
      "additionalFields",
      null,
      "com.adevinta",
      false,
      extraFields.asJava,
    )
  }
}

class SchemaExtender {
  import SchemaExtender._

  private val schemaCache = new ConcurrentHashMap[SchemaId, Schema]()

  def extendWithAdditionalFields(originalSchema: Schema, schemaId: SchemaId): Schema = {
    schemaCache.computeIfAbsent(
      schemaId,
      _ => {
        val fields = additionalFieldsSchema.getFields.asScala.map(deepCopyField) ++
          originalSchema.getFields.asScala.map(deepCopyField)
        Schema.createRecord(
          originalSchema.getName + "WithAdditionalFields",
          originalSchema.getDoc,
          originalSchema.getNamespace,
          false,
          fields.asJava,
        )
      },
    )
  }

  private def deepCopyField(field: Schema.Field): Schema.Field = {
    val copiedField =
      new Schema.Field(field.name, field.schema, field.doc, field.defaultVal, field.order)
    field.aliases.asScala.foreach(copiedField.addAlias)
    copiedField
  }

}
