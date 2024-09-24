package com.adevinta.bq.bqwriter

import scala.util.control.NoStackTrace

sealed trait AppError extends NoStackTrace

object AppError {
  final case class JsonDecodingError(message: String) extends Throwable(message) with AppError
}
