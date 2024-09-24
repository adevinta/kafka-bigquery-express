package com.adevinta.bq.bqwriter.job

import com.adevinta.bq.bqwriter.AppError.JsonDecodingError
import com.adevinta.bq.bqwriter.Fixtures

import java.nio.charset.StandardCharsets.UTF_8
import java.util.UUID
import zio.&
import zio.Clock
import zio.Scope
import zio.test.Assertion.anything
import zio.test.Assertion.fails
import zio.test.Assertion.isSubtype
import zio.test.assert
import zio.test.assertCompletes
import zio.test.assertTrue
import zio.test.Spec
import zio.test.TestEnvironment
import zio.test.ZIOSpecDefault
import com.adevinta.bq.bqwriter.AppError._
import Fixtures.JobPrefix
import com.adevinta.bq.bqwriter.GcsJobsRepositoryLive.BigQueryJobsState

object JobsSerdeSpec extends ZIOSpecDefault {
  private def generateId(): String = JobPrefix + "_" + UUID.randomUUID().toString

  override def spec: Spec[Scope & TestEnvironment, Any] = suite("BigQueryLoadJobsSerdeSpec")(
    test("serializes and deserializes BigQueryLoadJobs") {
      for {
        now <- Clock.instant
        allJobs = BigQueryJobsState(
          jobList = Vector(
            BigQueryJob(
              jobId = BQJobId(generateId()),
              tableId = Fixtures.testTableId,
              fileRefs = Vector(Fixtures.testFileRef),
              jobStatus = JobStatus.Success,
              creationTime = now,
              retryCount = 0
            )
          )
        )
        serializedJson <- JobsSerde.serialize(allJobs)
        deserializedAllJobs <- JobsSerde.deserialize(serializedJson)
      } yield assertCompletes && assertTrue(deserializedAllJobs == allJobs)
    },
    test("fails deserialization with JsonDecodingError if provided Json is not valid") {
      val invalidJsonBytes = "{invalid json}".getBytes(UTF_8)
      for {
        result <- JobsSerde.deserialize(invalidJsonBytes).exit
      } yield assert(result)(fails(isSubtype[JsonDecodingError](anything)))
    },
  )
}
