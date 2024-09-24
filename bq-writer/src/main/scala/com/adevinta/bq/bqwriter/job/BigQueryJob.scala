package com.adevinta.bq.bqwriter.job

import com.adevinta.bq.shared.remotedir.RemoteFileRef
import com.adevinta.bq.shared.remotedir.TableId

import java.time.Instant

final case class BigQueryJob(
    // Job ID that is unique to the project for its lifetime
    // to submit this job to BigQuery
    jobId: BQJobId,
    // Table ID is used as the key in the bq-writer
    // There can be one instance of BigQueryJob per table active at any given time
    tableId: TableId,
    fileRefs: Vector[RemoteFileRef],
    jobStatus: JobStatus,
    creationTime: Instant,
    retryCount: Int,
) {
  val jobUId: String = jobId.uid

  def retryJob(bqJobIdPrefix: String, creationTime: Instant): BigQueryJob =
    copy(
      jobId = BQJobId.generate(bqJobIdPrefix, tableId),
      jobStatus = JobStatus.New,
      creationTime = creationTime,
      retryCount = retryCount + 1
    )

  def isNew: Boolean = jobStatus == JobStatus.New
  def isSuccess: Boolean = jobStatus == JobStatus.Success
  def isFailed: Boolean = jobStatus.isFailed
  def isFinal: Boolean = jobStatus.isFinal

  override def toString: String =
    s"""BigQueryJob(id: $jobUId, tableId: $tableId, status: $jobStatus)"""

}

/** Id that is used to submit job in BigQuery
  *
  * @note
  *   It has to be unique for the lifetime of the project
  */
case class BQJobId(uid: String) extends AnyVal

object BQJobId {
  def generate(jobPrefix: String, tableId: TableId): BQJobId = {
    val jobId =
      s"${jobPrefix}_${tableId.datasetName}_${tableId.tableName}_${java.util.UUID.randomUUID().toString}"
    BQJobId(jobId)
  }
}

final case class JobError(
    reason: Option[String],
    location: Option[String],
    message: Option[String],
    debugInfo: Option[String],
)

object JobError {
  def apply(error: com.google.cloud.bigquery.BigQueryError): JobError = {
    JobError(
      Option(error.getReason),
      Option(error.getLocation),
      Option(error.getMessage),
      Option(error.getDebugInfo),
    )
  }
}

sealed trait JobStatus {
  def fold[A](
      whenNew: => A,
      whenPending: => A,
      whenRunning: => A,
      whenSuccess: => A,
      whenFailed: JobError => A,
  ): A =
    this match {
      case JobStatus.New           => whenNew
      case JobStatus.Pending       => whenPending
      case JobStatus.Running       => whenRunning
      case JobStatus.Success       => whenSuccess
      case JobStatus.Failed(error) => whenFailed(error)
    }

  val isSuccess: Boolean = {
    this match {
      case JobStatus.New       => false
      case JobStatus.Pending   => false
      case JobStatus.Running   => false
      case JobStatus.Success   => true
      case JobStatus.Failed(_) => false
    }
  }

  val isFailed: Boolean = {
    this match {
      case JobStatus.New       => false
      case JobStatus.Pending   => false
      case JobStatus.Running   => false
      case JobStatus.Success   => false
      case JobStatus.Failed(_) => true
    }
  }

  val isFinal: Boolean = {
    this match {
      case JobStatus.New       => false
      case JobStatus.Pending   => false
      case JobStatus.Running   => false
      case JobStatus.Success   => true
      case JobStatus.Failed(_) => true
    }
  }
}

object JobStatus {
  case object New extends JobStatus
  case object Pending extends JobStatus
  case object Running extends JobStatus
  case object Success extends JobStatus
  final case class Failed(error: JobError) extends JobStatus
}
