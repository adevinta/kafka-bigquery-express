package com.adevinta.bq.gcswriter.enricher

import TestSupport._
import com.adevinta.bq.gcswriter.NanoTime
import com.adevinta.bq.gcswriter.ParsedMessage
import com.adevinta.bq.gcswriter.SchemaId
import com.adevinta.bq.gcswriter.fixtures.TestRecord
import com.adevinta.bq.gcswriter.utils.AvroUtil
import AvroUtil.ArraySlice
import AvroUtil.recordToBytes
import org.apache.avro.generic.GenericRecord
import zio.test._
import zio.Chunk
import zio.Scope
import zio.Task
import zio.ZIO

import java.time.ZonedDateTime

object EventEnricherSpec extends ZIOSpecDefault {

  private val defaultOffset: Int = 10
  private val now: NanoTime = 79346345L
  private val schemaId: SchemaId = 52

  override def spec: Spec[TestEnvironment with Scope, Any] = {
    suite("EventEnricherSpec")(
      test(s"should enrich message") {
        val testRecord = TestRecord()
        for {
          parsedMessage <- fixtureToParsedMessage(testRecord)
          eventEnricher <- ZIO.service[EventEnricher]

          expectedDateTime <- getDateTime(testRecord)

          enrichedMessage <- eventEnricher.enrich(parsedMessage)
        } yield {
          assertFieldsMatch(
            message = enrichedMessage.head,
            expectedDateTime = expectedDateTime
          )
        }
      },
      test("should construct a valid parsed message containing the expected fields") {
        for {
          eventEnricher <- ZIO.service[EventEnricher]
          testRecord = TestRecord()

          parsedMessage <- fixtureToParsedMessage(
            testRecord,
            userAgentType = None,
            schemaId = schemaId,
            offset = defaultOffset,
            receivedAt = now
          )
          enrichedMessage <- eventEnricher.enrich(parsedMessage)

        } yield assertTrue(
          enrichedMessage.head.schemaId == schemaId,
          enrichedMessage.head.offset == defaultOffset,
          enrichedMessage.head.receivedAt == now
        )
      },
    )
      .provide(EventEnricherLive.layer)
  }

  private def getDateTime(fixture: GenericRecord): Task[Int] =
    AvroUtil.stringField(fixture, List("eventDateTime"))
      .map { value =>
        ZonedDateTime.parse(value.get)
          .toLocalDate.toEpochDay.toInt
      }

  private def fixtureToParsedMessage(
      fixture: GenericRecord,
      userAgentType: Option[String] = None,
      schemaId: Int = schemaId,
      offset: Int = defaultOffset,
      receivedAt: NanoTime = now
  ): Task[Chunk[ParsedMessage]] =
    recordToBytes(fixture).map { byteArray =>
      Chunk(
        ParsedMessage(
          ArraySlice(byteArray.toArray),
          fixture,
          userAgentType,
          schemaId,
          offset,
          receivedAt,
        )
      )
    }

  def assertFieldsMatch(
      message: EnrichedMessage,
      expectedDateTime: Int,
  ): TestResult = {

    val extendedEvent = deserializeEvent(
      message.extendedAvroBytes,
      message.extendedSchema
    )

    val obtainedExtraField = extendedEvent.getInt(SchemaExtender.EventDateFieldName)

    assertTrue(
      expectedDateTime == obtainedExtraField,
    )
  }

}
