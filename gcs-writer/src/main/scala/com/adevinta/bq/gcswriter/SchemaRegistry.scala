package com.adevinta.bq.gcswriter

import com.adevinta.bq.gcswriter.config.KafkaConfig
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig
import zio._

import scala.jdk.CollectionConverters._

object SchemaRegistry {

  val layer: URLayer[KafkaConfig, SchemaRegistryClient] = ZLayer {
    for {
      kafkaConfig <- ZIO.service[KafkaConfig]
    } yield {
      import kafkaConfig.schemaRegistryConfig._
      new CachedSchemaRegistryClient(
        baseUrl,
        200,
        Map(
          SchemaRegistryClientConfig.MISSING_SCHEMA_CACHE_TTL_CONFIG -> 5.minutes.toSeconds,
          SchemaRegistryClientConfig.MISSING_ID_CACHE_TTL_CONFIG -> 5.minutes.toSeconds,
          SchemaRegistryClientConfig.MISSING_CACHE_SIZE_CONFIG -> 200,
        ).asJava,
      )
    }
  }

}
