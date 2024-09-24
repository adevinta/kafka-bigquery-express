package com.adevinta.bq.gcswriter

import com.adevinta.bq.gcswriter.config.StartOffsetRetrieverConfig
import com.adevinta.bq.shared.remotedir.LocalDirectory
import com.adevinta.bq.shared.remotedir.RemoteDirectory
import com.adevinta.bq.shared.remotedir.TempLocalDirectoryConfig
import com.adevinta.zc.metrics.Metrics
import com.adevinta.zc.metrics.MetricsTest
import org.apache.kafka.common.TopicPartition
import zio._
import zio.test._

object StartOffsetRetrieverLiveSpec extends ZIOSpecDefault {

  private val datasetName = "animals_dataset"
  private val table = "cats_table"
  private val schemaId = 100
  private val firstOffset = 1
  private val noWaitConfig = StartOffsetRetrieverConfig(0.seconds)

  override def spec: Spec[TestEnvironment & Scope, Any] =
    suite("StartOffsetRetrieverLiveSpec")(
      test("should get the starting offset based on the objects in the remoteDirectory") {

        val topic1 = "europe_topic"
        val topic2 = "america_topic"

        val partition1 = 10
        val partition2 = 20

        val lastOffset1 = 50L
        val lastOffset2 = 10L
        val lastOffset3 = 80L

        val topicPartitions = Set(
          new TopicPartition(topic1, partition1),
          new TopicPartition(topic1, partition2),
          new TopicPartition(topic2, partition1),
        )

        val expected = Map(
          new TopicPartition(topic1, partition1) -> (lastOffset1 + 1L),
          new TopicPartition(topic1, partition2) -> (lastOffset2 + 1L),
          new TopicPartition(topic2, partition1) -> (lastOffset3 + 1L),
        )

        for {
          rd <- ZIO.service[RemoteDirectory]
          metrics <- ZIO.service[Metrics]
          startOffsetRetriever = new StartOffsetRetrieverLive(noWaitConfig, rd, metrics.registry)

          _ <- createEmptyGcsFile(
            rd,
            s"$topic1.$partition1.$datasetName.$table.$schemaId.$firstOffset.$lastOffset1.done"
          )
          _ <- createEmptyGcsFile(
            rd,
            s"$topic1.$partition2.$datasetName.$table.$schemaId.$firstOffset.$lastOffset2.done"
          )
          _ <- createEmptyGcsFile(
            rd,
            s"$topic2.$partition1.$datasetName.$table.$schemaId.$firstOffset.$lastOffset3.done"
          )

          result <- startOffsetRetriever.getOffset(topicPartitions)

        } yield assertTrue(result == expected)
      },
      test("should get the highest starting offset for every topic partition") {
        val topic = "europe_topic"
        val partition = 10
        val oldOffset1 = 51L
        val lastOffset = 52L
        val oldOffset2 = 50L

        val topicPartitions = Set(new TopicPartition(topic, partition))

        val expected = Map(
          new TopicPartition(topic, partition) -> (lastOffset + 1)
        )

        for {
          rd <- ZIO.service[RemoteDirectory]
          metrics <- ZIO.service[Metrics]
          startOffsetRetriever = new StartOffsetRetrieverLive(noWaitConfig, rd, metrics.registry)

          _ <- createEmptyGcsFile(
            rd,
            s"$topic.$partition.$datasetName.$table.$schemaId.$firstOffset.$oldOffset1.done"
          )
          _ <- createEmptyGcsFile(
            rd,
            s"$topic.$partition.$datasetName.$table.$schemaId.$firstOffset.$lastOffset.done"
          )
          _ <- createEmptyGcsFile(
            rd,
            s"$topic.$partition.$datasetName.$table.$schemaId.$firstOffset.$oldOffset2.done"
          )

          result <- startOffsetRetriever.getOffset(topicPartitions)

        } yield assertTrue(result == expected)
      },
      test(
        "should ignore objects with a file name that do not correspond to the expected pattern"
      ) {
        val topic = "europe_topic"
        val partition = 10
        val lastOffset = 60L

        val anotherOffset = 1000L

        val topicPartitions = Set(new TopicPartition(topic, partition))

        val expected = Map(
          new TopicPartition(topic, partition) -> (lastOffset + 1)
        )

        for {
          rd <- ZIO.service[RemoteDirectory]
          metrics <- ZIO.service[Metrics]
          startOffsetRetriever = new StartOffsetRetrieverLive(noWaitConfig, rd, metrics.registry)
          _ <- createEmptyGcsFile(
            rd,
            s"$topic.$partition.$datasetName.$table.$schemaId.$firstOffset.$lastOffset.done"
          )
          _ <- createEmptyGcsFile(
            rd,
            s"$topic.$partition.$datasetName.$table.$schemaId.$firstOffset.$anotherOffset.avro"
          )
          _ <- createEmptyGcsFile(
            rd,
            s"$topic.PARTITION.$datasetName.$table.$schemaId.$firstOffset.$anotherOffset.done"
          )
          _ <- createEmptyGcsFile(
            rd,
            s"$topic.$partition.$datasetName.$table.SCHEMAID.$firstOffset.$anotherOffset.done"
          )
          _ <- createEmptyGcsFile(
            rd,
            s"$topic.$partition.$datasetName.$table.$schemaId.FIRSTOFFSET.$anotherOffset.done"
          )
          _ <- createEmptyGcsFile(
            rd,
            s"$topic.$partition.$datasetName.$table.$schemaId.$firstOffset.ANOTHEROFFSET.done"
          )

          result <- startOffsetRetriever.getOffset(topicPartitions)

        } yield assertTrue(result == expected)
      },
      test("should delete all .avro files that do not have a counterpart .done") {

        val topic = "europe_topic"
        val anotherTopic = "america_topic"

        val partition = 10

        val lastOffset = 50L

        val topicPartitions = Set(
          new TopicPartition(topic, partition),
          new TopicPartition(anotherTopic, partition),
        )

        for {
          rd <- ZIO.service[RemoteDirectory]
          metrics <- ZIO.service[Metrics]
          startOffsetRetriever = new StartOffsetRetrieverLive(noWaitConfig, rd, metrics.registry)

          _ <- createEmptyGcsFile(
            rd,
            s"$topic.$partition.$datasetName.$table.$schemaId.$firstOffset.$lastOffset.done"
          )
          _ <- createEmptyGcsFile(
            rd,
            s"$topic.$partition.$datasetName.$table.$schemaId.$firstOffset.avro"
          )
          _ <- createEmptyGcsFile(
            rd,
            s"$anotherTopic.$partition.$datasetName.$table.$schemaId.$firstOffset.avro"
          )

          _ <- startOffsetRetriever.getOffset(topicPartitions)

          remainingFiles <- rd.list.runCollect

        } yield assertTrue(
          remainingFiles.length == 2,
          !remainingFiles.contains(
            s"$anotherTopic.$partition.$datasetName.$table.$schemaId.$firstOffset.avro"
          )
        )
      },
      test("should ignore file belonging to not relevant partitions") {

        val topic = "europe_topic"
        val anotherTopic = "america_topic"

        val partition = 10

        val lastOffset = 50L

        val topicPartitions = Set(
          new TopicPartition(topic, partition),
        )

        val expected = Map(
          new TopicPartition(topic, partition) -> (lastOffset + 1)
        )

        for {
          rd <- ZIO.service[RemoteDirectory]
          metrics <- ZIO.service[Metrics]
          startOffsetRetriever = new StartOffsetRetrieverLive(noWaitConfig, rd, metrics.registry)

          _ <- createEmptyGcsFile(
            rd,
            s"$topic.$partition.$datasetName.$table.$schemaId.$firstOffset.$lastOffset.done"
          )
          _ <- createEmptyGcsFile(
            rd,
            s"$anotherTopic.$partition.$datasetName.$table.$schemaId.$firstOffset.$lastOffset.done"
          )

          result <- startOffsetRetriever.getOffset(topicPartitions)

        } yield assertTrue(result == expected)
      },
    )
      .provideSome[Scope](
        TempLocalDirectoryConfig.layer,
        LocalDirectory.layer,
        MetricsTest.layer
      )

  private def createEmptyGcsFile(rd: RemoteDirectory, fileName: String) =
    rd.makeFile(s"staging/$fileName", Array.emptyByteArray)

}
