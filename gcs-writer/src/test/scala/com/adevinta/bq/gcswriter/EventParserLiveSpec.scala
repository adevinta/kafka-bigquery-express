package com.adevinta.bq.gcswriter

import com.adevinta.bq.gcswriter.fixtures.TestRecord
import com.adevinta.bq.gcswriter.mocks.MockSchemaRegistryClient
import io.confluent.kafka.schemaregistry.ParsedSchema
import io.confluent.kafka.schemaregistry.avro.AvroSchema
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaEntity
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference
import org.apache.avro.Schema
import org.apache.avro.io.EncoderFactory
import org.apache.avro.specific.SpecificData
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.avro.specific.SpecificRecord
import org.apache.avro.util.Utf8
import zio._
import zio.mock.Expectation
import zio.mock.Expectation._
import zio.test.Assertion._
import zio.test._

import java.io.ByteArrayOutputStream
import java.util
import java.util.HexFormat
import scala.util.control.NoStackTrace

object EventParserLiveSpec extends ZIOSpecDefault {
  private val offset: Long = 823435L
  private val now: NanoTime = 79346345L
  private val schemaId = 52
  private val validEvent = TestRecord()
  private val validRecord = makeConfluentBytesFromEvent(validEvent)
  private val incoming =
    IncomingKafkaMessage(validRecord, Map.empty, offset = offset, receivedAt = now, "topic")

  private val randomBytes = HexFormat.of().parseHex("21cc7f347030fb553de2f1b637ab916cf7e2505f")

  override def spec: Spec[TestEnvironment with Scope, Any] = {
    suite("EventParserLiveSpec")(
      test("deserializes a message") {
        val testProgram = for {
          eventParser <- ZIO.service[EventParser]
          parsed <- eventParser.parse(Chunk.single(incoming))
        } yield {
          val eventStr = parsed.headOption.map(_.genericRecord.toString)
          val expectedStr = validEvent.toString
          assertTrue(
            parsed.size == 1,
            eventStr.contains(expectedStr)
          )
        }

        testProgram.provide(
          mockSchemaRegistry(validEvent.getSchema()),
          EventParserLive.layer
        )
      },
      test("finds user agent header") {
        val botEvent = incoming.copy(headers = Map("useragenttype" -> new Utf8("bot")))
        val testProgram = for {
          eventParser <- ZIO.service[EventParser]
          parsed <- eventParser.parse(Chunk.single(botEvent))
        } yield {
          val userAgent = parsed.headOption.flatMap(_.userAgent)
          assertTrue(userAgent.contains("bot"))
        }

        testProgram.provide(
          mockSchemaRegistry(validEvent.getSchema()),
          EventParserLive.layer
        )
      },
      test("fails to deserialize random bytes") {
        assertParsingFails(randomBytes, MockSchemaRegistryClient.empty)
      },
      test("fails to deserialize with missing schema") {
        val exception = new RuntimeException("~unknown schema~") with NoStackTrace
        val expectation =
          MockSchemaRegistryClient.GetSchemaById(equalTo(schemaId), failure(exception))

        assertParsingFails(validRecord, expectation)
      },
      test("fails to deserialize with non avro schema") {
        // noinspection NotImplementedCode
        val schema = new ParsedSchema {
          override def schemaType(): String = "non-avro"
          override def name(): String = ???
          override def canonicalString(): String = ???
          override def references(): util.List[SchemaReference] = ???
          override def isBackwardCompatible(previousSchema: ParsedSchema): util.List[String] = ???
          override def rawSchema(): AnyRef = ???
          override def version(): Integer = ???
          override def metadata(): Metadata = ???
          override def ruleSet(): RuleSet = ???
          override def copy(): ParsedSchema = ???
          override def copy(version: Integer): ParsedSchema = ???
          override def copy(metadata: Metadata, ruleSet: RuleSet): ParsedSchema = ???
          override def copy(
              tagsToAdd: util.Map[SchemaEntity, util.Set[String]],
              tagsToRemove: util.Map[SchemaEntity, util.Set[String]]
          ): ParsedSchema = ???
        }
        val expectation = MockSchemaRegistryClient.GetSchemaById(equalTo(schemaId), value(schema))

        assertParsingFails(validRecord, expectation)
      },
      test("fails to deserialize broken avro bytes") {
        val recordBytes = Array[Byte](0, 0, 0, 0, schemaId.toByte) ++ randomBytes
        assertParsingFails(recordBytes, mockSchemaRegistry(validEvent.getSchema()))
      },
      test("fails to deserialize broken magic") {
        val recordBytes = Array[Byte](255.toByte, 0, 0, 0, schemaId.toByte) ++ randomBytes
        assertParsingFails(recordBytes, MockSchemaRegistryClient.empty)
      },
      test("fails to deserialize too short message") {
        val recordBytes = Array[Byte](0, 0, 0, 0)
        assertParsingFails(recordBytes, MockSchemaRegistryClient.empty)
      }
    )
  }

  private def assertParsingFails(
      record: Array[Byte],
      schemaExpectation: ULayer[SchemaRegistryClient]
  ): ZIO[Any, Throwable, TestResult] = {
    val testProgram = for {
      epl <- ZIO.service[EventParser]
      result <- epl.parse(
        Chunk.single(
          IncomingKafkaMessage(record, Map.empty, offset = offset, receivedAt = now, "topic")
        )
      ).exit
    } yield assertTrue(result.isFailure)

    testProgram.provide(
      schemaExpectation,
      EventParserLive.layer
    )
  }

  private def mockSchemaRegistry(schema: Schema): Expectation[SchemaRegistryClient] = {
    val parsedSchema = new AvroSchema(schema)
    MockSchemaRegistryClient.GetSchemaById(equalTo(schemaId), value(parsedSchema))
  }

  private def makeConfluentBytesFromEvent[T <: SpecificRecord](event: T): Array[Byte] =
    Array[Byte](0, 0, 0, 0, schemaId.toByte) ++ toAvroBytes(event)

  private def toAvroBytes[T <: SpecificRecord](event: T): Array[Byte] = {
    val schema = event.getSchema
    val writer = new SpecificDatumWriter[T](schema, SpecificData.get)
    val byteArrayOutputStream = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().directBinaryEncoder(byteArrayOutputStream, null)
    writer.write(event, encoder)
    encoder.flush()
    byteArrayOutputStream.toByteArray
  }

}
