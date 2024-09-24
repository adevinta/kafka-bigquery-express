package com.adevinta.bq.gcswriter

import com.adevinta.bq.gcswriter.enricher.EnrichedMessage
import com.adevinta.bq.gcswriter.fixtures.TestRecord
import com.adevinta.bq.gcswriter.utils.AvroUtil
import com.adevinta.bq.shared.remotedir.LocalDirectory
import com.adevinta.bq.shared.remotedir.RemoteDirectory
import com.adevinta.bq.shared.remotedir.TempLocalDirectoryConfig
import com.adevinta.zc.metrics.Metrics
import com.adevinta.zc.metrics.MetricsTest
import com.softwaremill.quicklens.ModifyPimp
import org.apache.avro.file.DataFileReader
import org.apache.avro.file.SeekableByteArrayInput
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.EncoderFactory
import org.apache.kafka.common.TopicPartition
import zio._
import zio.stream._
import zio.test._

import java.io.ByteArrayOutputStream
import scala.util.Using

object AvroFileWriterLiveSpec extends ZIOSpecDefault {

  private val schemaId: SchemaId = 162
  private val epoch: NanoTime = 1000000L

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("AvroFileWriterLiveSpec")(
      test("writeToAvroFile writes to a single Avro file") {
        val tp10 = new TopicPartition("topic1", 0)
        val events = makeTestEvents(1000)
        val enrichedMessages = makeEnrichedMessages(events)
        for {
          rd <- ZIO.service[RemoteDirectory]
          metrics <- ZIO.service[Metrics]
          latencies <- {
            val enrichedMessageStream = ZStream(enrichedMessages).rechunk(10)
            val avroFileWriter = new AvroFileWriterLive(rd, metrics.registry, () => 5.minutes)
            val outStream = avroFileWriter.writeToAvroFile(tp10, enrichedMessageStream)
            outStream.runCollect
          }
          list <- rd.list.runCollect
          // _ <- Console.printLine(list.mkString("\n"))
          avroFileContent <- rd.readFile("staging/topic1.0.animals.giraffes.162.0.avro")
        } yield {
          val latencyCount = latencies.flatten.size
          val readEvents = avroFileReader(avroFileContent)
          val eventCount = readEvents.size
          val allEventsSeen = haveSameRequestIds(readEvents, events)
          assertTrue(
            list.contains("staging/topic1.0.animals.giraffes.162.0.avro"),
            list.contains("staging/topic1.0.animals.giraffes.162.0.999.done"),
            latencyCount == 1,
            eventCount == 1000,
            allEventsSeen,
          )
        }
      },
      test("writeToAvroFile writes events with different schema ids to a different Avro files") {
        val tp44 = new TopicPartition("topic4", 4)
        val events = makeTestEvents(1000)
        val schemaIds: Seq[SchemaId] = Seq(814, 980)
        val schemaIdIter = Iterator.from(0).map(i => schemaIds(i % schemaIds.size))
        val enrichedMessages = makeEnrichedMessages(events)
          .map(_.copy(schemaId = schemaIdIter.next()))
        for {
          rd <- ZIO.service[RemoteDirectory]
          metrics <- ZIO.service[Metrics]
          latencies <- {
            val enrichedMessageStream = ZStream(enrichedMessages).rechunk(10)
            val avroFileWriter = new AvroFileWriterLive(rd, metrics.registry, () => 5.minutes)
            val outStream = avroFileWriter.writeToAvroFile(tp44, enrichedMessageStream)
            outStream.runCollect
          }
          list <- rd.list.runCollect
          // _ <- Console.printLine(list.mkString("\n"))
          avroFileContent814 <- rd.readFile("staging/topic4.4.animals.giraffes.814.0.avro")
          avroFileContent980 <- rd.readFile("staging/topic4.4.animals.giraffes.980.0.avro")
        } yield {
          val latencyCount = latencies.flatten.size
          val events814 = avroFileReader(avroFileContent814)
          val events980 = avroFileReader(avroFileContent980)
          val eventCount = events814.size + events980.size
          val allEventsSeen = haveSameRequestIds(events814 ++ events980, events)
          assertTrue(
            list.contains("staging/topic4.4.animals.giraffes.814.0.avro"),
            list.contains("staging/topic4.4.animals.giraffes.814.0.999.done"),
            list.contains("staging/topic4.4.animals.giraffes.980.0.avro"),
            list.contains("staging/topic4.4.animals.giraffes.980.0.999.done"),
            latencyCount == 1,
            eventCount == 1000,
            allEventsSeen,
          )
        }
      },
      test(
        "writeToAvroFile writes events with different dataset or table to different Avro files"
      ) {
        val tp77 = new TopicPartition("topic7", 7)
        val events = makeTestEvents(1000)
        val tableIds = Seq(("livestock", "chicken"), ("livestock", "lama"), ("zoo-animals", "lama"))
        val tableItIter = Iterator.from(0).map(i => tableIds(i % tableIds.size))
        val enrichedMessages = makeEnrichedMessages(events)
          .map { em =>
            val (datasetName, tableName) = tableItIter.next()
            em.copy(datasetName = datasetName, tableName = tableName)
          }
        for {
          rd <- ZIO.service[RemoteDirectory]
          metrics <- ZIO.service[Metrics]
          latencies <- {
            val enrichedMessageStream = ZStream(enrichedMessages).rechunk(10)
            val avroFileWriter = new AvroFileWriterLive(rd, metrics.registry, () => 5.minutes)
            val outStream = avroFileWriter.writeToAvroFile(tp77, enrichedMessageStream)
            outStream.runCollect
          }
          list <- rd.list.runCollect
          // _ <- Console.printLine(list.mkString("\n"))
          avroFileContentLC <- rd.readFile("staging/topic7.7.livestock.chicken.162.0.avro")
          avroFileContentLL <- rd.readFile("staging/topic7.7.livestock.lama.162.0.avro")
          avroFileContentZL <- rd.readFile("staging/topic7.7.zoo-animals.lama.162.0.avro")
        } yield {
          val latencyCount = latencies.flatten.size
          val eventsLC = avroFileReader(avroFileContentLC)
          val eventsLL = avroFileReader(avroFileContentLL)
          val eventsZL = avroFileReader(avroFileContentZL)
          val eventCount = eventsLC.size + eventsLL.size + eventsZL.size
          val allEventsSeen = haveSameRequestIds(eventsLC ++ eventsLL ++ eventsZL, events)
          assertTrue(
            list.contains("staging/topic7.7.livestock.chicken.162.0.avro"),
            list.contains("staging/topic7.7.livestock.chicken.162.0.999.done"),
            list.contains("staging/topic7.7.livestock.lama.162.0.avro"),
            list.contains("staging/topic7.7.livestock.lama.162.0.999.done"),
            list.contains("staging/topic7.7.zoo-animals.lama.162.0.avro"),
            list.contains("staging/topic7.7.zoo-animals.lama.162.0.999.done"),
            latencyCount == 1,
            eventCount == 1000,
            allEventsSeen,
          )
        }
      },
      test("writeToAvroFile writes events to multiple Avro files over time") {
        val tp30 = new TopicPartition("topic3", 0)
        val events = makeTestEvents(1000)
        val enrichedMessages = makeEnrichedMessages(events)
        for {
          rd <- ZIO.service[RemoteDirectory]
          metrics <- ZIO.service[Metrics]
          queue <- Queue.unbounded[Chunk[EnrichedMessage]]
          latenciesF <- {
            val enrichedMessageStream = ZStream.fromQueue(queue)
            val avroFileWriter = new AvroFileWriterLive(rd, metrics.registry, () => 5.minutes)
            val outStream = avroFileWriter.writeToAvroFile(tp30, enrichedMessageStream)
            outStream.runCollect
          }.fork
          chunks = enrichedMessages.grouped(300).toIndexedSeq
          _ <- ZIO.foreachDiscard(chunks) { chunk =>
            queue.offer(chunk) <* TestClock.adjust(5.minutes)
          }
          _ <- queue.shutdown
          latencies <- latenciesF.join
          list <- rd.list.runCollect
          // _ <- Console.printLine(list.mkString("\n"))
          avroFileContent1 <- rd.readFile("staging/topic3.0.animals.giraffes.162.0.avro")
          avroFileContent2 <- rd.readFile("staging/topic3.0.animals.giraffes.162.300.avro")
          avroFileContent3 <- rd.readFile("staging/topic3.0.animals.giraffes.162.600.avro")
          avroFileContent4 <- rd.readFile("staging/topic3.0.animals.giraffes.162.900.avro")
        } yield {
          val latencyCount = latencies.flatten.size
          val readEvents1 = avroFileReader(avroFileContent1)
          val readEvents2 = avroFileReader(avroFileContent2)
          val readEvents3 = avroFileReader(avroFileContent3)
          val readEvents4 = avroFileReader(avroFileContent4)
          val eventCount = readEvents1.size + readEvents2.size + readEvents3.size + readEvents4.size
          val allEventsSeen =
            haveSameRequestIds(readEvents1 ++ readEvents2 ++ readEvents3 ++ readEvents4, events)
          assertTrue(
            list.contains("staging/topic3.0.animals.giraffes.162.0.avro"),
            list.contains("staging/topic3.0.animals.giraffes.162.0.299.done"),
            list.contains("staging/topic3.0.animals.giraffes.162.300.avro"),
            list.contains("staging/topic3.0.animals.giraffes.162.300.599.done"),
            list.contains("staging/topic3.0.animals.giraffes.162.600.avro"),
            list.contains("staging/topic3.0.animals.giraffes.162.600.899.done"),
            list.contains("staging/topic3.0.animals.giraffes.162.900.avro"),
            list.contains("staging/topic3.0.animals.giraffes.162.900.999.done"),
            latencyCount == 4,
            eventCount == 1000,
            allEventsSeen,
          )
        }
      },
    )
      .provideSome[Scope](
        TempLocalDirectoryConfig.layer,
        LocalDirectory.layer,
        MetricsTest.layer,
      )

  private def makeTestEvents(n: Int): Chunk[TestRecord] = {
    Chunk.tabulate(n) { i =>
      TestRecord()
        .modify(_.requestId)
        .setTo(f"$i%016d")
    }
  }

  private def makeEnrichedMessages(events: Chunk[TestRecord]): Chunk[EnrichedMessage] = {
    events.zipWithIndex.map { case (event, i) =>
      makeEnrichedMessage(
        offset = i,
        receivedAt = epoch + i * 1000,
        event = event,
      )
    }
  }

  private def makeEnrichedMessage(
      offset: Long,
      receivedAt: Long,
      event: TestRecord,
  ): EnrichedMessage = {
    EnrichedMessage(
      schemaId = schemaId,
      extendedAvroBytes = Chunk.fromArray(recordToBytes(event)),
      extendedSchema = event.getSchema,
      datasetName = "animals",
      tableName = "giraffes",
      offset = offset,
      receivedAt = receivedAt,
    )
  }

  private def recordToBytes(genericRecord: GenericRecord): Array[Byte] = {
    val datumWriter = new GenericDatumWriter[GenericRecord](genericRecord.getSchema)
    Using.resource(new ByteArrayOutputStream()) { bos =>
      val enc = EncoderFactory.get().directBinaryEncoder(bos, null)
      datumWriter.write(genericRecord, enc)
      bos.flush()
      bos.toByteArray
    }
  }

  private def avroFileReader(avroFileContent: Array[Byte]): Chunk[GenericRecord] = {
    val seekableInput = new SeekableByteArrayInput(avroFileContent)
    val genericReader =
      new DataFileReader[GenericRecord](seekableInput, new GenericDatumReader[GenericRecord]())
    Chunk.fromJavaIterator(genericReader.iterator())
  }

  private def haveSameRequestIds(
      genericRecords: Chunk[GenericRecord],
      sentEvents: Chunk[TestRecord],
  ): Boolean = {
    genericRecords
      .map(gc => AvroUtil.stringFieldUnsafe(gc, List("requestId")))
      .flatten
      .sorted ==
      sentEvents.map(_.requestId)
  }

}
