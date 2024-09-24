package com.adevinta.bq.gcswriter

import com.adevinta.bq.gcswriter.enricher.EnrichedMessage
import ZstreamOps.Ops
import com.adevinta.bq.shared.remotedir.RemoteDirectory
import com.adevinta.zc.metrics.Metrics
import io.prometheus.client.CollectorRegistry
import io.prometheus.client.Counter
import org.apache.avro.file.CodecFactory
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.TopicPartition
import zio._
import zio.stream._

import java.nio.ByteBuffer

trait AvroFileWriter {

  /** Write enriched messages to an Avro file.
    *
    * @param tp
    *   the topic/partition
    * @param stream
    *   a stream with events for topic/partition `tp`
    * @return
    *   a stream with the latency (time received to time to close Avro file) for each enriched
    *   message Note: this stream only emits elements when an Avro file is closed.
    */
  def writeToAvroFile(
      tp: TopicPartition,
      stream: ZStream[Any, Throwable, Chunk[EnrichedMessage]]
  ): ZStream[Any, Throwable, Chunk[NanoTime]]
}

object AvroFileWriterLive {
  val layer: ZLayer[RemoteDirectory & Metrics, Throwable, AvroFileWriter] = ZLayer {
    for {
      remoteDirectory <- ZIO.service[RemoteDirectory]
      metrics <- ZIO.service[Metrics]
    } yield AvroFileWriterLive(remoteDirectory, metrics.registry, closeDelay)
  }

  // Close the file after this time has passed.
  private def closeDelay(): Duration = {
    // Write to the same file for a duration drawn from a uniform distribution from 4 to 6 minutes
    (4.minutes.toMillis + 2.minutes.toMillis * scala.util.Random.nextDouble()).toLong.millis
  }
}

final case class AvroFileWriterLive(
    remoteDirectory: RemoteDirectory,
    registry: CollectorRegistry,
    closeDelay: () => Duration,
) extends AvroFileWriter {
  import AvroFileWriterLive._

  private val avroFileOpenedCounter = Counter
    .build("gcswriter_filecount", "Number of Avro files opened")
    .register(registry)

  private val avroFileClosedCounter = Counter
    .build("gcswriter_closedfilecount", "Number of Avro files closed")
    .register(registry)

  override def writeToAvroFile(
      tp: TopicPartition,
      stream: ZStream[Any, Throwable, Chunk[EnrichedMessage]]
  ): ZStream[Any, Throwable, Chunk[NanoTime]] = {
    val closeFileMarker = Chunk.empty[EnrichedMessage]
    stream
      .filter(_.nonEmpty)
      .timeInterleaved(closeDelay(), closeFileMarker)
      .mapAccumZIO(PartitionState.empty) { (state, enrichedMessages) =>
        NonEmptyChunk.fromChunk(enrichedMessages) match {
          case Some(enrichedMessages) =>
            // Write the messages, update the state. Emit no latencies yet.
            state.appendMessages(tp, enrichedMessages).map((_, Chunk.empty))
          case None =>
            // Close all writers. Then start with a fresh empty state and emit the latencies.
            state.closeAvroWriters.map((PartitionState.empty, _))
        }
      }
  }

  // Each stream carries traffic for 1 partition, therefore we can use one PartitionState instance per stream.
  // Since a partition can contain events from multiple dataset/table/schema-id, while these need to go
  // to different Avro files, we have one AvroWriter per dataset/table/schema-id (see `writerKey` below).
  //
  // All `.avro` and `.done` files written concurrently from the same partition, will get the same start and end offsets
  // in their file name.
  //
  // Not `final`, see https://stackoverflow.com/a/16466541/1262728
  private case class PartitionState(
      firstOffset: Long,
      lastOffset: Long,
      avroWriters: Map[String, AvroWriter],
      firstReceivedAt: Option[NanoTime]
  ) {

    /** Append each message to an Avro file, return the updated state. */
    def appendMessages(
        tp: TopicPartition,
        enrichedMessages: NonEmptyChunk[EnrichedMessage],
    ): ZIO[Any, Throwable, PartitionState] = {
      for {
        state <- ZIO.foldLeft(enrichedMessages)(this) { (state, enrichedMessage) =>
          state.append(tp, enrichedMessage)
        }
      } yield {
        // For efficiency, we update lastOffset and firstReceivedAt here, in one go, and not one-by-one in `append`.
        state.copy(
          lastOffset = enrichedMessages.last.offset,
          firstReceivedAt = this.firstReceivedAt.orElse(Some(enrichedMessages.head.receivedAt)),
        )
      }
    }

    private def append(
        tp: TopicPartition,
        enrichedMessage: EnrichedMessage,
    ): ZIO[Any, Throwable, PartitionState] = {
      def registerNewAvroWriter(
          firstOffset: Long,
          writerKey: String,
          avroWriter: AvroWriter
      ): PartitionState =
        copy(
          firstOffset = firstOffset,
          avroWriters = avroWriters.updated(writerKey, avroWriter)
        )

      val writerKey =
        s"${enrichedMessage.datasetName}_${enrichedMessage.tableName}_${enrichedMessage.schemaId}"
      for {
        (avroWriter, state) <- ZIO.fromOption(avroWriters.get(writerKey))
          .map((_, this))
          .orElse {
            val firstOffset =
              if (this.firstOffset >= 0) this.firstOffset
              else enrichedMessage.offset
            makeAvroWriter(tp, firstOffset, enrichedMessage)
              .map { avroWriter =>
                (avroWriter, registerNewAvroWriter(firstOffset, writerKey, avroWriter))
              }
              .zipLeft(ZIO.succeed(avroFileOpenedCounter.inc()))
          }
        _ <- avroWriter.append(enrichedMessage)
      } yield state
    }

    private def makeAvroWriter(
        tp: TopicPartition,
        firstOffset: Long,
        enrichedMessage: EnrichedMessage,
    ): ZIO[Any, Throwable, AvroWriter] = {
      val schema = enrichedMessage.extendedSchema
      val fileName = {
        import enrichedMessage._
        val topic = tp.topic()
        val partition = tp.partition()

        // We're using `.` as separator, so it is not allowed in any of the file name parts.
        assert(!topic.contains('.'), "Period `.` is not supported in topic name")
        assert(!datasetName.contains('.'), "Period `.` is not supported in dataset name")
        assert(!tableName.contains('.'), "Period `.` is not supported in table name")

        // Note: we are using `firstOffset` and not enrichedMessage.offset so that all files written from the same
        // offset range have the same offset in the file name.
        s"staging/$topic.$partition.$datasetName.$tableName.$schemaId.$firstOffset.avro"
      }
      for {
        outputStream <- remoteDirectory.makeFileFromStream(fileName)
        writer <- ZIO.attemptBlocking {
          val compressionCodec = CodecFactory.deflateCodec(CodecFactory.DEFAULT_DEFLATE_LEVEL)
          val writer =
            new DataFileWriter[GenericRecord](new GenericDatumWriter[GenericRecord](schema))
          writer.setCodec(compressionCodec)
          writer.create(schema, outputStream)
        }
      } yield AvroWriter(fileName, writer)
    }

    // Close all currently open Avro writers, and return latency of the first message.
    def closeAvroWriters: Task[Chunk[NanoTime]] = {
      for {
        _ <- ZIO.foreachDiscard(avroWriters.values)(_.close(this.lastOffset))
        now <- Clock.nanoTime
      } yield {
        val latencies = firstReceivedAt.map(now - _)
        Chunk.empty ++ latencies
      }
    }
  }

  private object PartitionState {
    val empty: PartitionState = PartitionState(-1, -1, Map.empty, None)
  }

  // Not `final`, see https://stackoverflow.com/a/16466541/1262728
  private case class AvroWriter(
      fileName: String,
      avroWriter: DataFileWriter[GenericRecord]
  ) {
    def append(enrichedMessage: EnrichedMessage): Task[Unit] = ZIO.attemptBlocking {
      avroWriter.appendEncoded(ByteBuffer.wrap(enrichedMessage.extendedAvroBytes.toArray))
    }

    def close(lastOffset: Long): Task[Unit] = {
      val doneFileName = s"${fileName.stripSuffix(".avro")}.$lastOffset.done"
      ZIO.attemptBlocking(avroWriter.close()) *>
        remoteDirectory.makeFile(doneFileName, Array.emptyByteArray) <*
        ZIO.succeed(avroFileClosedCounter.inc())
    }
  }

}
