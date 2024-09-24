package com.adevinta.bq

import com.adevinta.bq.shared.remotedir.GcsFileName
import org.apache.kafka.common.TopicPartition

package object gcswriter {

  /** Schema id as given by Confluent's Schema Registry. */
  type SchemaId = Int

  type NanoTime = Long

  implicit class NanoTimeOps(nanoTime: NanoTime) {
    def toSeconds: Double = nanoTime.toDouble / 1e9
  }

  implicit class RichGcsFileName(val gcsFileName: GcsFileName) extends AnyVal {
    def topicPartition = new TopicPartition(gcsFileName.topicName, gcsFileName.partition)
  }
}
