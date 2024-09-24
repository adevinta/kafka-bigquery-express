package com.adevinta.bq.bqwriter

import com.adevinta.bq.shared.remotedir.AvroGcsFileName
import com.adevinta.bq.shared.remotedir.DoneGcsFileName
import com.adevinta.bq.shared.remotedir.RemoteFileRef
import java.time.Instant

object TestSupport {

  def makeRemoteFileRef(
      fileName: String,
      prefix: String = "test-data",
      creationTime: Instant = Instant.EPOCH
  ): RemoteFileRef =
    RemoteFileRef(
      schema = "gs",
      host = "kafka-bigquery-testbucket",
      prefix = prefix,
      fileName = s"staging/$fileName",
      creationTime
    )

  def makeAvroFileName(
      topicName: String = "europe_topic",
      datasetName: String = "animals_dataset",
      tableName: String = "cats_table",
      partition: Int = 1,
      firstOffset: Long = 1001L,
      schemaId: Int = 1,
      pathPrefix: String = "test-data",
      creationTime: Instant = Instant.EPOCH,
  ): AvroGcsFileName =
    AvroGcsFileName(
      makeRemoteFileRef(
        s"$topicName.$partition.$datasetName.$tableName.$schemaId.$firstOffset.avro",
        pathPrefix,
        creationTime
      ),
      topicName,
      partition,
      datasetName,
      tableName,
      schemaId,
      firstOffset,
    )

  def makeAvroDoneFileName(
      topicName: String,
      partition: Int = 1,
      datasetName: String,
      tableName: String,
      schemaId: Int = 1,
      firstOffset: Int = 1001,
      lastOffset: Int = 2001,
      pathPrefix: String = "test-data",
      creationTime: Instant = Instant.EPOCH,
  ): DoneGcsFileName = DoneGcsFileName(
    makeRemoteFileRef(
      s"$topicName.$partition.$datasetName.$tableName.$schemaId.$firstOffset.avro",
      pathPrefix,
      creationTime
    ),
    topicName,
    partition,
    datasetName,
    tableName,
    schemaId,
    firstOffset,
    lastOffset
  )
}
