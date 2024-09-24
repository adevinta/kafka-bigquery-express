package com.adevinta.bq.bqwriter.job

import com.adevinta.bq.bqwriter.AppError.JsonDecodingError
import com.adevinta.bq.bqwriter.GcsJobsRepositoryLive.BigQueryJobsState
import com.adevinta.bq.bqwriter.AppError.JsonDecodingError
import com.github.plokhotnyuk.jsoniter_scala.core._
import zio._
import com.adevinta.bq.bqwriter.GcsJobsRepositoryLive.BigQueryJobsState

object JobsSerde {

  def serialize(jobs: BigQueryJobsState): Task[Array[Byte]] = ZIO.attempt(writeToArray(jobs))

  def deserialize(json: Array[Byte]): IO[JsonDecodingError, BigQueryJobsState] =
    ZIO.attempt(readFromArray[BigQueryJobsState](json))
      .refineOrDie[JsonDecodingError] { case jre: JsonReaderException =>
        JsonDecodingError(s"Invalid BigQueryJobsState: not valid JSON '${jre.getMessage}'")
      }

}
