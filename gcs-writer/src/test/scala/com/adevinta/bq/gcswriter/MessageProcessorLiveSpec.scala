package com.adevinta.bq.gcswriter

import com.adevinta.bq.gcswriter.config.KafkaConfig
import com.adevinta.bq.gcswriter.config.SchemaRegistryConfig
import com.adevinta.bq.gcswriter.enricher.EnrichedMessage
import com.adevinta.bq.gcswriter.enricher.EventEnricher
import com.adevinta.bq.gcswriter.fixtures.TestRecord
import com.adevinta.bq.gcswriter.ziokafka.KafkaZIOSpec
import com.adevinta.bq.gcswriter.utils.AvroUtil.ArraySlice
import com.adevinta.zc.metrics.MetricsTest
import com.softwaremill.quicklens._
import org.apache.avro.Schema
import org.apache.avro.util.Utf8
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import zio.ZIO
import zio.ZLayer
import zio._
import zio.kafka.testkit.Kafka
import zio.kafka.testkit.KafkaTestUtils._
import zio.stream.ZStream
import zio.test.TestAspect.timeout
import zio.test.TestAspect.withLiveClock
import zio.test._

import org.apache.avro.io.BinaryEncoder
import org.apache.avro.io.EncoderFactory
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.avro.specific.SpecificRecord
import java.io.ByteArrayOutputStream

//noinspection ConvertExpressionToSAM
object MessageProcessorLiveSpec extends KafkaZIOSpec {

  override def spec: Spec[TestEnvironment with Kafka, Any] = {
    suite("MessageProcessorLiveSpec")(
      test("consumes some messages, parse, enriches and publishes those") {
        val messageCount = 5000
        // The mock event parser always generates an Event, the eventId is set to the incoming message
        val mockEventParser = new EventParser {
          override def parse(
              data: Chunk[IncomingKafkaMessage]
          ): ZIO[Any, Throwable, Chunk[ParsedMessage]] =
            ZIO.succeed {
              data.map { message =>
                val event =
                  TestRecord().modify(_.eventId).setTo(new Utf8(message.value))
                val avroBytes = specificRecordToBytes(event)
                ParsedMessage(
                  avroBytes = ArraySlice(avroBytes),
                  genericRecord = event,
                  userAgent = None,
                  schemaId = 83,
                  offset = message.offset,
                  receivedAt = message.receivedAt,
                )
              }
            }
        }
        // The mock enricher always sets the user agent to "enriched"
        // Also, it sets the extended bytes to the eventId (getting back the original record value)
        val mockEventEnricher: EventEnricher = new EventEnricher {
          override def enrich(
              events: Chunk[ParsedMessage]
          ): ZIO[Any, Throwable, Chunk[EnrichedMessage]] =
            ZIO.succeed {
              events.map { e =>
                val enrichedEvent = e.genericRecord.asInstanceOf[TestRecord]
                  .modify(_.userAgent).setTo(Some(new Utf8("enricher")))

                EnrichedMessage(
                  schemaId = e.schemaId,
                  extendedAvroBytes = Chunk.fromArray(enrichedEvent.eventId.getBytes),
                  extendedSchema = Schema.create(Schema.Type.STRING),
                  datasetName = enrichedEvent.datasetName.get.toString,
                  tableName = enrichedEvent.tableName.get.toString,
                  offset = e.offset,
                  receivedAt = e.receivedAt,
                )
              }
            }
        }
        val mockStartOffsetRetriever = new StartOffsetRetriever {
          override def getOffset(
              topicPartitions: Set[TopicPartition]
          ): Task[Map[TopicPartition, Long]] = ZIO.succeed(
            topicPartitions.map(elem => elem -> 0L).toMap
          )
        }
        def makeMockAvroFileWriter(
            published: Ref[Chunk[(TopicPartition, EnrichedMessage)]]
        ): AvroFileWriter = new AvroFileWriter {
          override def writeToAvroFile(
              tp: TopicPartition,
              stream: ZStream[Any, Throwable, Chunk[EnrichedMessage]]
          ): ZStream[Any, Throwable, Chunk[NanoTime]] = {
            stream
              .tap(msg => published.update(_ ++ (msg.map(tp -> _))))
              .as(Chunk.empty)
          }
        }
        val kvs = Chunk.fromIterable(1 to messageCount).map(i => (s"key$i", s"msg$i"))

        for {
          publishedMessages <- Ref.make[Chunk[(TopicPartition, EnrichedMessage)]](Chunk.empty)
          mockAvroFileWriter = makeMockAvroFileWriter(publishedMessages)
          _ <- createTopic("destination", partitions = 10)
          _ <- produceMany("destination", kvs).provideSome[Kafka](producer)
          fib <- MessageProcessor.run().fork
            .provideSome[Kafka](
              ZLayer {
                ZIO.environmentWith[Kafka](_.get[Kafka].bootstrapServers)
                  .map(kafkaConfig("destination", "run-groupId-839"))
              },
              ZLayer.succeed[StartOffsetRetriever](mockStartOffsetRetriever),
              ZLayer.succeed[EventParser](mockEventParser),
              ZLayer.succeed[EventEnricher](mockEventEnricher),
              ZLayer.succeed[AvroFileWriter](mockAvroFileWriter),
              MetricsTest.layer,
              MessageProcessorLive.layer,
            )
          _ <- ZIO.sleep(10.millis).repeatUntilZIO { _ =>
            publishedMessages.get.map(_.size == messageCount)
          }
          _ <- fib.interrupt
          // All consumer records must be converted and published:
          inputValues = kvs.map(_._2).sorted
          publishedMessages <- publishedMessages.get
          eventIds = publishedMessages
            .map(e => new String(e._2.extendedAvroBytes.toArray))
            .sorted
          enrichedCount = publishedMessages.size
          topicsPublishedTo = publishedMessages.map(_._1.topic()).distinct
          partitionsPublishedTo = publishedMessages.map(_._1.partition()).distinct.sorted
          partitionsAreValid = partitionsPublishedTo.forall(p => Range(0, 10).contains(p))
        } yield {
          assertTrue(
            inputValues.size == enrichedCount,
            eventIds == inputValues,
            topicsPublishedTo == Chunk("destination"),
            partitionsAreValid
          )
        }
      } @@ withLiveClock @@ timeout(1.minute)
    )
  }

  private def kafkaConfig(topic: String, groupId: String)(bootstrapServers: List[String]) = {
    KafkaConfig(
      sourceTopics = NonEmptyChunk(topic),
      bootstrapServers,
      credentials = None,
      groupId = groupId,
      extraConsumerProperties = Map(
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest"
      ),
      schemaRegistryConfig = SchemaRegistryConfig("???"),
    )
  }

  private def specificRecordToBytes[T <: SpecificRecord](record: T): Array[Byte] = {
    val writer = new SpecificDatumWriter[T](record.getSchema) // Create a writer for the record
    val outputStream = new ByteArrayOutputStream()
    val encoder: BinaryEncoder = EncoderFactory.get().binaryEncoder(outputStream, null)

    writer.write(record, encoder)
    encoder.flush()
    outputStream.close()

    outputStream.toByteArray
  }

}
