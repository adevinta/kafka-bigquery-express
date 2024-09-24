package com.adevinta.bq.bqwriter

import com.adevinta.bq.bqwriter.config.BigQueryConfig
import com.adevinta.bq.bqwriter.job.BQJobId
import com.adevinta.bq.bqwriter.job.BigQueryJob
import com.adevinta.bq.bqwriter.job.JobError
import com.adevinta.bq.bqwriter.job.JobStatus

import java.time.Instant
import scala.jdk.CollectionConverters._
import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.BigQueryOptions
import com.google.cloud.bigquery.FormatOptions
import com.google.cloud.bigquery.JobId
import com.google.cloud.bigquery.{Job => BQJob}
import com.google.cloud.bigquery.{JobInfo => BQJobInfo}
import com.google.cloud.bigquery.{JobStatus => BQJobStatus}
import com.google.cloud.bigquery.{LoadJobConfiguration => BQLoadJobConfiguration}
import com.google.cloud.bigquery.{TableId => BQTableId}
import com.google.cloud.bigquery.{TimePartitioning => BQTimePartitioning}
import com.google.common.collect.ImmutableList
import zio._
import com.adevinta.zc.metrics.Metrics

/** Service that can submit and fetch job status from BigQuery. */
trait BigQueryJobService {

  /** Fetches the status of the given jobs from BigQuery, and returns these jobs with the updated
    * status.
    */
  def jobsWithUpdatedStatus(
      jobIdPrefix: String,
      runningJobs: Chunk[BigQueryJob]
  ): Task[Chunk[BigQueryJob]]

  /* Submits new load jobs, returns the created jobs. */
  def submitJobs(newJobs: Chunk[BigQueryJob]): ZIO[Any, Throwable, Unit]
}

object BigQueryJobServiceLive {

  /** Gives access to underlying BigQuery API. */
  val bigQueryLayer: ZLayer[BigQueryConfig, Throwable, BigQuery] = ZLayer {
    for {
      config <- ZIO.service[BigQueryConfig]
      credentials <- config.gcpCredentials.credentials
      bigquery <- ZIO.attemptBlocking(
        BigQueryOptions
          .newBuilder()
          .setProjectId(config.projectIdString)
          .setLocation(config.location)
          .setCredentials(credentials)
          .build()
          .getService
      )
    } yield bigquery
  }

  private type Dependencies = BigQueryConfig with BigQuery with JobsRepository with Metrics

  val layer: ZLayer[Dependencies, Nothing, BigQueryJobService] =
    ZLayer {
      for {
        config <- ZIO.service[BigQueryConfig]
        jobsRepository <- ZIO.service[JobsRepository]
        bigQuery <- ZIO.service[BigQuery]
      } yield {
        BigQueryJobServiceLive(
          config,
          jobsRepository,
          bigQuery
        )
      }
    }
}

final case class BigQueryJobServiceLive(
    config: BigQueryConfig,
    jobsRepository: JobsRepository,
    bigQuery: BigQuery
) extends BigQueryJobService {

  override def jobsWithUpdatedStatus(
      jobIdPrefix: String,
      runningJobs: Chunk[BigQueryJob]
  ): Task[Chunk[BigQueryJob]] =
    if (runningJobs.isEmpty) ZIO.succeed(runningJobs)
    else {
      val creationTimeOfOldestJob = runningJobs.map(_.creationTime).min
      for {
        jobsStatuses <- fetchJobsStatuses(jobIdPrefix, creationTimeOfOldestJob)
        _ <- ZIO.foreachDiscard(jobsStatuses) { case (jobId, jobStatus) =>
          jobStatus match {
            case JobStatus.Failed(jobError) =>
              ZIO.logWarning(s"Job $jobId failed with error $jobError")
            case _ => ZIO.unit
          }
        }
      } yield {
        runningJobs.map { job =>
          jobsStatuses.get(job.jobId).fold(job)(status => job.copy(jobStatus = status))
        }
      }
    }

  override def submitJobs(newJobs: Chunk[BigQueryJob]): ZIO[Any, Throwable, Unit] =
    ZIO
      .foreachDiscard(newJobs) { newJob =>
        ZIO.attemptBlocking {
          val jobInfo = BQJobInfo
            .newBuilder(makeLoadJobConfiguration(newJob))
            .setJobId(JobId.of(config.projectIdString, newJob.jobUId))
            .build()

          // Creates BQ Job
          bigQuery.create(jobInfo)
        }
      }
      .unit

  /** Fetches the job status from BigQuery. */
  // package private access for testing
  private[bqwriter] def fetchJobsStatuses(
      jobPrefix: String,
      creationTimeOfOldestJob: Instant,
  ): Task[Map[BQJobId, JobStatus]] =
    ZIO.attemptBlocking {
      bigQuery
        .listJobs(
          BigQuery.JobListOption.minCreationTime(creationTimeOfOldestJob.toEpochMilli),
          BigQuery.JobListOption.fields(BigQuery.JobField.ID, BigQuery.JobField.STATUS),
        )
        .iterateAll()
        .asScala
        .filter(_.getJobId.getJob.startsWith(jobPrefix))
        .map(j => BQJobId(j.getJobId.getJob) -> getJobStatus(j))
        .toMap
    }
      .retry(Schedule.fibonacci(100.milli) && Schedule.recurs(3))

  private def getJobStatus(job: BQJob): JobStatus =
    job.getStatus.getState match {
      case BQJobStatus.State.RUNNING => JobStatus.Running
      case BQJobStatus.State.PENDING => JobStatus.Pending
      case BQJobStatus.State.DONE =>
        Option(job.getStatus.getError)
          .fold[JobStatus](JobStatus.Success)(e => JobStatus.Failed(JobError(e)))
    }

  private[bqwriter] def makeLoadJobConfiguration(job: BigQueryJob): BQLoadJobConfiguration = {
    val destinationTableId: BQTableId = BQTableId.of(job.tableId.datasetName, job.tableId.tableName)
    val gcsFileUris = job.fileRefs.map(_.uri).asJava
    BQLoadJobConfiguration
      .newBuilder(destinationTableId, gcsFileUris)
      .setFormatOptions(FormatOptions.avro)
      .setUseAvroLogicalTypes(true)
      .setSchemaUpdateOptions(
        ImmutableList.of(
          BQJobInfo.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
          BQJobInfo.SchemaUpdateOption.ALLOW_FIELD_RELAXATION,
        )
      )
      .setTimePartitioning(
        BQTimePartitioning
          .newBuilder(BQTimePartitioning.Type.DAY)
          .setField("eventDate")
          .setExpirationMs(config.partitionRetentionInDays.days.toMillis)
          .build()
      )
      .build()
  }
}
