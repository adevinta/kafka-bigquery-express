package com.adevinta.bq.gcswriter.config

import com.adevinta.bq.shared.remotedir.GcsRemoteDirectoryConfig
import com.adevinta.bq.shared.remotedir.RemoteDirectoryConfig
import com.adevinta.zc.config.Secret
import com.adevinta.zc.http.HttpConfig
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.security.scram.ScramLoginModule
import org.apache.kafka.common.security.scram.internals.ScramMechanism
import zio._
import zio.kafka.consumer.Consumer.OffsetRetrieval
import zio.kafka.consumer.ConsumerSettings
import zio.kafka.consumer.fetch.ManyPartitionsQueueSizeBasedFetchStrategy

final case class AppConfig(
    httpConfig: HttpConfig,
    startOffsetRetrieverConfig: StartOffsetRetrieverConfig,
    kafkaConfig: KafkaConfig,
    remoteDirectoryConfig: RemoteDirectoryConfig,
    gcsRemoteDirectoryConfig: GcsRemoteDirectoryConfig,
)

final case class StartOffsetRetrieverConfig(waitTime: Duration)

final case class KafkaCredentials(username: String, password: Secret)

final case class SchemaRegistryConfig(baseUrl: String)

final case class KafkaConfig(
    sourceTopics: NonEmptyChunk[String],
    bootstrapServers: List[String],
    credentials: Option[KafkaCredentials],
    groupId: String,
    schemaRegistryConfig: SchemaRegistryConfig,
    extraConsumerProperties: Map[String, String] = Map.empty
) {
  // We select 20 seconds max rebalance duration, meaning the streams have 20 seconds to process
  // already fetched records.
  // Please validate if 20 seconds is enough for your workload. With your throughput per pod, and amount of traffic
  // per pod, you can calculate if already fetched records can be processed before the deadline.
  def makeConsumerSetting(
      offsetExtraction: Set[TopicPartition] => Task[Map[TopicPartition, Long]]
  ): ConsumerSettings =
    ConsumerSettings(bootstrapServers)
      .withPollTimeout(500.millis)
      .withMaxPollRecords(250)
      .withFetchStrategy(ManyPartitionsQueueSizeBasedFetchStrategy(250, 5000))
      .withGroupId(groupId)
      // Custom offset retrieval for every partition, if not provided, it will use the earliest offset
      .withProperties(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest")
      .withOffsetRetrieval(OffsetRetrieval.Manual(offsetExtraction))
      .withRebalanceSafeCommits(true)
      .withMaxRebalanceDuration(20.seconds)
      .withProperties(saslProperties ++ assignorProperties ++ extraConsumerProperties)

  private def saslProperties: Map[String, String] = {
    credentials match {
      case None => Map.empty
      case Some(KafkaCredentials(username, Secret(password))) =>
        val loginModuleName = classOf[ScramLoginModule].getName
        Map(
          CommonClientConfigs.SECURITY_PROTOCOL_CONFIG -> SecurityProtocol.SASL_SSL.name,
          SaslConfigs.SASL_MECHANISM -> ScramMechanism.SCRAM_SHA_256.mechanismName(),
          SaslConfigs.SASL_JAAS_CONFIG -> s"""$loginModuleName required username="$username" password="$password";"""
        )
    }
  }

  private val assignorProperties: Map[String, String] = Map(
    ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG -> "org.apache.kafka.clients.consumer.CooperativeStickyAssignor"
  )
}
