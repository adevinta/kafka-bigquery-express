package com.adevinta.bq.gcswriter.ziokafka

import zio._
import zio.kafka.admin.AdminClient
import zio.kafka.testkit.Kafka
import zio.kafka.testkit.KafkaTestUtils._
import zio.kafka.testkit.ZIOSpecWithKafka

trait KafkaZIOSpec extends ZIOSpecWithKafka {

  def createTopic(
      topic: String,
      topicConfig: Map[String, String] = Map.empty,
      partitions: Int = 1,
      replicationFactor: Short = 1
  ): RIO[Kafka, Unit] =
    withAdmin { adminClient =>
      val newTopic = AdminClient.NewTopic(topic, partitions, replicationFactor, topicConfig)
      adminClient.createTopic(newTopic)
    }

}
