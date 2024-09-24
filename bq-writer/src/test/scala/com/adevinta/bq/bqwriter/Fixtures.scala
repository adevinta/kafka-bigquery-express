package com.adevinta.bq.bqwriter

import java.time.Instant
import java.util.UUID
import TestSupport.makeRemoteFileRef
import com.adevinta.bq.bqwriter.job.JobStatus
import com.adevinta.bq.bqwriter.job.BQJobId
import com.adevinta.bq.bqwriter.job.BigQueryJob
import com.adevinta.bq.shared.remotedir.RemoteFileRef
import com.adevinta.bq.shared.remotedir.TableId
import zio.Chunk

object Fixtures {

  val ProjectID = "nonprod"
  val JobPrefix = "bq-writer-test"
  val testTopicName = "animals-topic"
  val testDatasetName = "animals_dataset"
  val testTableName = "cats_table"
  val testFirstOffset = 2001

  def generateId(): String = JobPrefix + "_" + UUID.randomUUID().toString

  def jobId(tableId: TableId): BQJobId =
    BQJobId.generate(JobPrefix, tableId)

  val testTableId: TableId =
    TableId(testDatasetName, testTableName)

  val testFileRef: RemoteFileRef =
    makeRemoteFileRef(s"$testTopicName.1.$testDatasetName.$testTableName.2.1001.avro")

  val testDuplicateFileRef: RemoteFileRef =
    makeRemoteFileRef(s"$testTopicName.1.$testDatasetName.$testTableName.1.$testFirstOffset.avro")

  val newJobFileRefs: Vector[RemoteFileRef] =
    Vector(
      makeRemoteFileRef(s"$testTopicName.1.$testDatasetName.$testTableName.1.1001.avro"),
      testDuplicateFileRef,
    )

  val completedJobFileRefs: Vector[RemoteFileRef] = Vector(
    makeRemoteFileRef(s"$testTopicName.1.$testDatasetName.$testTableName.1.1230.avro"),
    // Note: Following two are duplicates.
    makeRemoteFileRef(s"$testTopicName.1.$testDatasetName.$testTableName.1.9000.avro"),
    makeRemoteFileRef(s"$testTopicName.1.$testDatasetName.$testTableName.1.9000.avro"),
  )

  val testSubmittedJob: BigQueryJob = BigQueryJob(
    jobId = jobId(testTableId),
    tableId = testTableId,
    fileRefs = Vector(testFileRef),
    jobStatus = JobStatus.Pending,
    creationTime = Instant.EPOCH,
    retryCount = 0
  )

  val testNewJob: BigQueryJob = BigQueryJob(
    jobId = jobId(testTableId),
    tableId = testTableId,
    fileRefs = newJobFileRefs,
    jobStatus = JobStatus.New,
    creationTime = Instant.EPOCH,
    retryCount = 0
  )

  val testSuccessfulJob: BigQueryJob = BigQueryJob(
    jobId = jobId(testTableId),
    tableId = TableId(testDatasetName, testTableName),
    fileRefs = completedJobFileRefs,
    jobStatus = JobStatus.Success,
    creationTime = Instant.EPOCH,
    retryCount = 0
  )

  val testRunningJob: BigQueryJob = BigQueryJob(
    jobId = jobId(testTableId),
    tableId = testTableId,
    fileRefs = Vector(testFileRef),
    jobStatus = JobStatus.Running,
    creationTime = Instant.EPOCH,
    retryCount = 0
  )

  val testJobs: Chunk[BigQueryJob] = Chunk(testSuccessfulJob, testSubmittedJob)
  val testNewJobs: Chunk[BigQueryJob] = Chunk(testNewJob)
  val testRunningJobs: Chunk[BigQueryJob] = Chunk(testRunningJob)
  val testCompletedJobs: Chunk[BigQueryJob] = Chunk(testSuccessfulJob)
}
