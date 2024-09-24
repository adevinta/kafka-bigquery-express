package com.adevinta.bq.gcswriter

import com.adevinta.bq.gcswriter.config.KafkaConfig
import com.adevinta.bq.gcswriter.enricher.EnrichedMessage
import com.adevinta.bq.gcswriter.enricher.EventEnricher
import com.adevinta.zc.metrics.Metrics
import com.adevinta.zc.metrics.ZioInstrumented.DefaultBatchSizeBuckets
import io.prometheus.client.CollectorRegistry
import io.prometheus.client.Counter
import io.prometheus.client.Histogram
import org.apache.avro.util.Utf8
import org.apache.kafka.common.TopicPartition
import zio._
import zio.kafka.consumer.CommittableRecord
import zio.kafka.consumer.Consumer
import zio.kafka.consumer.Subscription
import zio.kafka.serde.Serde
import zio.stream.ZStream

import scala.jdk.CollectionConverters._

/** Consumes from Kafka, enriches event, produce to GCS. */
trait MessageProcessor {
  def run(): ZIO[Any, Throwable, Unit]
  def isHealthy: UIO[Boolean]
}

object MessageProcessor {
  def run(): ZIO[MessageProcessor, Throwable, Unit] =
    ZIO.serviceWithZIO[MessageProcessor](_.run())
}

object MessageProcessorLive {
  private type Dependencies =
    KafkaConfig
      with StartOffsetRetriever
      with EventParser
      with EventEnricher
      with AvroFileWriter
      with Metrics

  val layer: ZLayer[Dependencies, Any, MessageProcessor] = ZLayer {
    for {
      kafkaConfig <- ZIO.service[KafkaConfig]
      startOffsetRetriever <- ZIO.service[StartOffsetRetriever]
      eventParser <- ZIO.service[EventParser]
      eventEnricher <- ZIO.service[EventEnricher]
      avroFileWriter <- ZIO.service[AvroFileWriter]
      metrics <- ZIO.service[Metrics]
    } yield MessageProcessorLive(
      kafkaConfig,
      startOffsetRetriever,
      eventParser,
      eventEnricher,
      avroFileWriter,
      metrics.registry,
    )
  }

  private implicit class ZStreamAvroOps(val stream: ZStream[Any, Throwable, Chunk[EnrichedMessage]])
      extends AnyVal {
    def writeToAvroFile(tp: TopicPartition)(implicit
        avroFileWriter: AvroFileWriter
    ): ZStream[Any, Throwable, Chunk[NanoTime]] =
      avroFileWriter.writeToAvroFile(tp, stream)
  }
}

final case class MessageProcessorLive(
    kafkaConfig: KafkaConfig,
    startOffsetRetriever: StartOffsetRetriever,
    eventParser: EventParser,
    eventEnricher: EventEnricher,
    avroFileWriter: AvroFileWriter,
    registry: CollectorRegistry,
) extends MessageProcessor {
  import MessageProcessorLive._

  private implicit val afw: AvroFileWriter = avroFileWriter

  private val LatencyTimerBuckets: Array[Double] =
    Array(30.0, 60.0, 120.0, 180.0, 240.0, 300.0, 600.0)

  private val recordCounter = Counter
    .build("gcswriter_consumedmessages", "Number of messages read from Kafka")
    .register(registry)

  private val batchSizeHistogram = Histogram
    .build("gcswriter_publishbatchsize", "Publish batch size")
    .buckets(DefaultBatchSizeBuckets: _*)
    .register(registry)

  private val latencyTimer = Histogram
    .build("gcswriter_latency", "Latency of processing a message")
    .buckets(LatencyTimerBuckets: _*)
    .register(registry)

  private val bytesProcessed = Counter
    .build("gcswriter_bytes_written", "Avro bytes written to GCS (before compression) per dataset")
    .labelNames("dataset")
    .register(registry)

  override def run(): ZIO[Any, Throwable, Unit] = ZIO.scoped {
    Consumer.make(kafkaConfig.makeConsumerSetting(startOffsetRetriever.getOffset))
      .flatMap { consumer =>
        val subscription = Subscription.Topics(kafkaConfig.sourceTopics.toSet)
        consumer
          .partitionedStream(subscription, Serde.byteArray, Serde.byteArray)
          .flatMapPar(Int.MaxValue) { case (tp, stream) =>
            stream
              .chunks
              .mapZIO { records =>
                for {
                  now <- Clock.nanoTime
                  incoming = records.map(cr => incomingKafkaMessage(cr, now))
                  parsed <- eventParser.parse(incoming)
                  enriched <- eventEnricher.enrich(parsed)
                  _ <- ZIO.succeed(recordCounter.inc(records.size))
                } yield enriched
              }
              .tap(incrementMetricsAfterEnrich)
              .writeToAvroFile(tp)
              .tap { latencies =>
                ZIO.succeed(latencies.foreach(l => latencyTimer.observe(l.toSeconds)))
              }
          }
          .runDrain
      }
      .onError {
        case cause if cause.isInterruptedOnly => ZIO.unit
        case cause => ZIO.logErrorCause(s"Failed to process a message", cause)
      }
  }

  private def incrementMetricsAfterEnrich(enrichedMessages: Chunk[EnrichedMessage]): UIO[Unit] =
    ZIO.succeed {
      batchSizeHistogram.observe(enrichedMessages.size)

      enrichedMessages.foreach { enrichedMessage =>
        bytesProcessed
          .labels(enrichedMessage.datasetName)
          .inc(enrichedMessage.extendedAvroBytes.size)
      }
    }

  private def incomingKafkaMessage(
      cr: CommittableRecord[Array[Byte], Array[Byte]],
      receivedAt: NanoTime
  ): IncomingKafkaMessage = {
    val headers = cr.record.headers().asScala.map(h => h.key -> new Utf8(h.value)).toMap
    IncomingKafkaMessage(cr.value, headers, cr.offset.offset, receivedAt, cr.record.topic())
  }

  override def isHealthy: UIO[Boolean] =
    ZIO.succeed(true)
}
