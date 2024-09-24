package com.adevinta.bq.bqwriter.job

import com.adevinta.bq.bqwriter.Fixtures
import com.adevinta.bq.bqwriter.job.JobStatus.New
import com.adevinta.bq.bqwriter.job.JobStatus.Pending
import com.adevinta.bq.bqwriter.job.JobStatus.Running
import com.adevinta.bq.bqwriter.job.JobStatus.Success

import java.time.Instant
import com.github.plokhotnyuk.jsoniter_scala.core._
import com.github.plokhotnyuk.jsoniter_scala.macros.CodecMakerConfig
import com.github.plokhotnyuk.jsoniter_scala.macros.JsonCodecMaker
import com.google.cloud.bigquery.BigQueryError
import zio._
import zio.test._
import zio.test.Assertion._
import com.adevinta.bq.bqwriter.GcsJobsRepositoryLive.BigQueryJobsState
import JobStatus._
import com.adevinta.bq.shared.remotedir.RemoteFileRef
import com.adevinta.bq.shared.remotedir.TableId

object BigQueryJobsSpec extends ZIOSpecDefault {
  implicit val bqLoadJobCodec: JsonValueCodec[BigQueryJob] =
    JsonCodecMaker.make(CodecMakerConfig)
  implicit val tableIdCodec: JsonValueCodec[TableId] = JsonCodecMaker.make(CodecMakerConfig)
  implicit val blobIdCodec: JsonValueCodec[RemoteFileRef] = JsonCodecMaker.make(CodecMakerConfig)
  implicit val jobStatusCodec: JsonValueCodec[JobStatus] = JsonCodecMaker.make(CodecMakerConfig)
  implicit val jobErrorCodec: JsonValueCodec[JobError] = JsonCodecMaker.make(CodecMakerConfig)

  override def spec: Spec[TestEnvironment with Scope, Any] = suite("BigQueryLoadJobsSpec")(
    test("serializes and deserializes an instance of BigQueryJobStatus") {
      val statuses: Vector[JobStatus] = Vector(
        Pending,
        Running,
        Success,
        Failed(
          JobError(
            Some("reason"),
            None,
            Some("message"),
            None
          )
        ),
      )

      val outcome: ZIO[Any, Throwable, Boolean] =
        ZIO
          .foreach(statuses) { status =>
            val json: String = writeToString(status)
            for {
              deserializedStatus <- ZIO.attempt(readFromString[JobStatus](json))
              statusEquals = deserializedStatus == status
              jsonEquals = json == expectedJson(status)
            } yield statusEquals && jsonEquals
          }
          .map(_.forall(identity))

      assertZIO(outcome)(isTrue)
    },
    test("serializes and deserializes an instance of TableId") {
      val tableId = TableId("testDataset", "testTable")
      validateCodecWorks(tableId)
    },
    test("serializes and deserializes an instance of BlobId") {
      val remoteFile = Fixtures.testFileRef
      validateCodecWorks(remoteFile)
    },
    test("serializes and deserializes an instance of BigQueryLoadJob") {
      val loadJob = BigQueryJob(
        BQJobId(Fixtures.generateId()),
        TableId("animals_dataset", "cats_table"),
        Vector(Fixtures.testFileRef),
        Success,
        Instant.ofEpochMilli(1234567890L),
        retryCount = 0
      )
      validateCodecWorks(loadJob)
    },
    test("serializes and deserializes an instance of BigQueryLoadJobs") {
      val loadJobs = BigQueryJobsState(
        jobList = Vector(
          BigQueryJob(
            BQJobId(Fixtures.generateId()),
            tableId = TableId("animals_dataset", "cats_table"),
            fileRefs = Vector(Fixtures.testFileRef),
            jobStatus = Success,
            creationTime = Instant.ofEpochMilli(1234567890L),
            retryCount = 0
          )
        )
      )
      validateCodecWorks(loadJobs)
    },
    test(
      "serializes and deserializes an instance of BigQueryLoadJobs with JobStatus Failure that contains null error values"
    ) {
      val loadJobs = BigQueryJobsState(
        jobList = Vector(
          BigQueryJob(
            BQJobId(Fixtures.generateId()),
            tableId = TableId("animals_dataset", "cats_table"),
            fileRefs = Vector(Fixtures.testFileRef),
            jobStatus = JobStatus.Failed(JobError(new BigQueryError(null, null, null, null))),
            creationTime = Instant.ofEpochMilli(1234567890L),
            retryCount = 0
          )
        )
      )
      validateCodecWorks(loadJobs)
    }
  )

  def validateCodecWorks[A: JsonValueCodec](value: A): ZIO[Any, Throwable, TestResult] = {
    val jsonString: String = writeToString(value)
    val deserializedValue =
      ZIO.attempt(readFromString[A](jsonString))

    assertZIO(deserializedValue)(equalTo(value))
  }

  def expectedJson(status: JobStatus): String = status match {
    case New     => """{"type":"New"}"""
    case Pending => """{"type":"Pending"}"""
    case Running => """{"type":"Running"}"""
    case Success => """{"type":"Success"}"""
    case Failed(_) =>
      """{"type":"Failed","error":{"reason":"reason","message":"message"}}"""

  }
}
