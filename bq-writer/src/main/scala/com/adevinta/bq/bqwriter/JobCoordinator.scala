package com.adevinta.bq.bqwriter

import com.adevinta.bq.bqwriter.config.JobCoordinatorConfig
import com.adevinta.bq.bqwriter.job.BQJobId
import com.adevinta.bq.bqwriter.job.BigQueryJob
import com.adevinta.bq.bqwriter.job.EnrichedBigQueryJobs
import com.adevinta.bq.bqwriter.job.JobStatus
import com.adevinta.bq.shared.remotedir.AvroGcsFileName
import com.adevinta.bq.shared.remotedir.TableId
import com.adevinta.zc.metrics.Metrics
import zio._
import zio.stream._
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.time.Instant
import scala.math.Ordered.orderingToOrdered
import io.prometheus.client.CollectorRegistry
import io.prometheus.client.Counter
import io.prometheus.client.Gauge

import scala.util.control.NoStackTrace

/** Computes the jobs to be scheduled and initiates processing of them via [[BigQueryJobService]].
  */
trait JobCoordinator {

  /** Runs the main loop based on the schedule specified by `jobSubmissionPolicy` to fetch the files
    * from [[com.adevinta.bq.bqwriter.StagingRepository]] and computes the new jobs that can
    * scheduled and initiate processing of the jobs via [[BigQueryJobService]].
    *
    * Might fail with a `JobRunningTooLong` or `TooManyRetries` error.
    */
  def runLoop(): Task[Unit]
}

object JobCoordinator {
  object JobRunningTooLong
      extends RuntimeException(
        "Some jobs are running too long (see logs), cannot recover, aborting..."
      )
        with NoStackTrace
  object TooManyRetries
      extends RuntimeException("Some jobs failed too often (see logs), cannot recover, aborting...")
        with NoStackTrace
}

object JobCoordinatorLive {
  // We use an old-fashioned logger because its much more convenient in pure functions.
  private val logger: Logger = LoggerFactory.getLogger(classOf[JobCoordinatorLive])

  // Maximum number of source URIs in job configuration	10,000 URIs
  // Details: https://cloud.google.com/bigquery/quotas#load_jobs
  private val MaximumSourceUriCount: Int = 10000

  private type Dependencies = BigQueryJobService
    with JobsRepository
    with StagingRepository
    with JobCoordinatorConfig
    with Metrics

  val layer: ZLayer[Dependencies, Any, JobCoordinatorLive] = ZLayer {
    for {
      bigQueryJobService <- ZIO.service[BigQueryJobService]
      jobRepository <- ZIO.service[JobsRepository]
      stagingRepository <- ZIO.service[StagingRepository]
      config <- ZIO.service[JobCoordinatorConfig]
      metrics <- ZIO.service[Metrics]
    } yield JobCoordinatorLive(
      bigQueryJobService,
      jobRepository,
      stagingRepository,
      config,
      metrics.registry
    )
  }

  private sealed trait JobCoordinatorTask extends Product with Serializable
  private case object ScheduleJobs extends JobCoordinatorTask
  private case object CheckJobs extends JobCoordinatorTask

  /** To prevent schema selection problems in BigQuery, we make sure jobs always have Avro files
    * that all have the same schema.
    *
    * @return
    *   the first `DefaultBQJobIdPrefix` avro files that have the same (lowest) schema
    */
  // package private for testing
  private[bqwriter] def selectCompatibleAvroFilesNew(
      avroFiles: Chunk[AvroGcsFileName]
  ): Chunk[AvroGcsFileName] = {
    val groupedBySchemaId = avroFiles.groupBy(_.schemaId)

    val avroFilesWithMinSchemaId =
      if (groupedBySchemaId.size <= 1) {
        avroFiles
      } else {
        val lowestSchemaId = groupedBySchemaId.keys.min
        val refs = groupedBySchemaId(lowestSchemaId)
        logger.info(
          s"Found Avro files with different schema ids: ${groupedBySchemaId.keySet}. " +
            s"Selecting only the ${refs.size} files with the lowest schema id: $lowestSchemaId"
        )
        refs
      }

    avroFilesWithMinSchemaId.take(MaximumSourceUriCount)
  }
}

case class JobCoordinatorLive(
    bigQueryJobService: BigQueryJobService,
    jobRepository: JobsRepository,
    stagingRepository: StagingRepository,
    config: JobCoordinatorConfig,
    registry: CollectorRegistry
) extends JobCoordinator {

  import JobCoordinatorLive._

  private val submittedJobsToBqCounter = Counter
    .build(
      "bqwriter_submittedjobstobq",
      "Number of BigQuery load jobs submitted"
    )
    .register(registry)

  private val failedJobsCounter: Counter = Counter
    .build(
      "bqwriter_failed_bqjobs",
      "Number of failed BigQuery load jobs"
    )
    .register(registry)

  private val jobsCountGauge: Gauge = Gauge
    .build(
      "bqwriter_bqjobs_count",
      "Number of running BigQuery load jobs"
    )
    .register(registry)

  override def runLoop(): Task[Unit] =
    runLoopWith(
      jobSubmissionPolicy = Schedule.spaced(config.jobSubmissionInterval).jittered(0.9, 1.1),
      jobCheckPolicy = Schedule.spaced(config.jobCheckInterval).jittered(0.9, 1.1),
    )

  // package private for testing
  private[bqwriter] def runLoopWith(
      jobSubmissionPolicy: Schedule[Any, Any, Any],
      jobCheckPolicy: Schedule[Any, Any, Any],
  ): ZIO[Any, Throwable, Unit] = {
    for {
      initialJobs <- jobRepository.getJobs
      initialState = EnrichedBigQueryJobs.make(initialJobs)
      afterSubmitNewJobs <- reSubmitNewJobs(initialState).tap(observeJobCount)
      _ <- ZStream
        .mergeAllUnbounded()(
          ZStream.fromSchedule(jobSubmissionPolicy).as(JobCoordinatorLive.ScheduleJobs),
          ZStream.fromSchedule(jobCheckPolicy).as(JobCoordinatorLive.CheckJobs),
        )
        .runFoldZIO(afterSubmitNewJobs) { case (state, task) =>
          val taskZio = task match {
            case JobCoordinatorLive.ScheduleJobs => scheduleJobs(state)
            case JobCoordinatorLive.CheckJobs    => checkJobs(state)
          }
          taskZio.tap(observeJobCount)
        }
    } yield ()
  }

  private def updateStatusForJobs(
      jobs: Chunk[BigQueryJob]
  ): ZIO[Any, Throwable, Chunk[BigQueryJob]] =
    for {
      updatedJobs <- bigQueryJobService.jobsWithUpdatedStatus(config.jobPrefix, jobs)
      _ <- ZIO.succeed(
        failedJobsCounter.inc(updatedJobs.count(_.isFailed) - jobs.count(_.isFailed))
      )
    } yield updatedJobs

  /** Submit jobs that are in the NEW state. These jobs were probably (but not guaranteed) already
    * submitted by the previous instance of this service. Therefore, it is not a problem if these
    * jobs already exist.
    */
  // package private for testing
  private[bqwriter] def reSubmitNewJobs(state: EnrichedBigQueryJobs): Task[EnrichedBigQueryJobs] = {
    for {
      updatedJobs <- updateStatusForJobs(state.jobs)
      newJobs = updatedJobs.filter(_.isNew)
      afterSubmitState <- submitJobs("resubmitted", state, newJobs)
    } yield afterSubmitState
  }

  /** Schedule BigQuery Load Jobs based on the available files in the staging directory, and already
    * running load jobs.
    *
    * @note
    *   At any point of time, there can be only one job per table in the in progress state.
    */
  // package private for testing
  private[bqwriter] def scheduleJobs(state: EnrichedBigQueryJobs): Task[EnrichedBigQueryJobs] =
    for {
      // Fetches the Avro files
      avroFiles <- stagingRepository.listDoneAvroFiles()
        .retry(remoteDirectoryRetryPolicy)

      // To make sure we never load files twice, we remove any file that is already referenced by an active load job.
      avroFilesInActiveLoadJobFileRefs = state.avroFilesInActiveLoadJobFileRefs
      newAvroFiles = avroFiles.filter(fileName =>
        !avroFilesInActiveLoadJobFileRefs.contains(fileName.remoteFileRef)
      )

      // When a load job for a table is in progress, we do not start new jobs for that table
      tableIdsInActiveLoadJobs = state.tableIdsInActiveLoadJobs
      usableAvroFiles = newAvroFiles.filter(name =>
        !tableIdsInActiveLoadJobs.contains(name.tableId)
      )
      _ <- ZIO.when(usableAvroFiles.size != newAvroFiles.size) {
        ZIO.logWarning(
          s"Skipping ${newAvroFiles.size - usableAvroFiles.size} files because a load job is still running for the target table"
        )
      }

      // Create jobs to handle new Avro files
      now <- ZIO.clockWith(_.instant)
      newJobs = createJobsForNewAvroFiles(now, usableAvroFiles)
      afterSubmitState <- submitJobs("new", state, newJobs)
    } yield afterSubmitState

  /** Check the status of submitted BigQuery load jobs.
    *
    * If a job completes successfully:
    *   - Delete the Avro files that were loaded into BQ.
    *   - Remove the job from the state.
    *
    * If a job failed:
    *   - Retry the job.
    *   - Replace the job in the state.
    */
  // package private for testing
  private[bqwriter] def checkJobs(state: EnrichedBigQueryJobs): Task[EnrichedBigQueryJobs] = {
    for {
      updatedJobs <- updateStatusForJobs(state.jobs)

      _ <- failOnTooLongRunningJobs(updatedJobs)

      // Delete Avro files for completed jobs
      successJobs = updatedJobs.filter(_.isSuccess)
      processedFileRefs = successJobs.flatMap(_.fileRefs)
      _ <- stagingRepository.deleteFiles(processedFileRefs)
        .retry(remoteDirectoryRetryPolicy)
        .when(processedFileRefs.nonEmpty)
      successJobsRemoved = state.jobsRemoved(successJobs)
      _ <- jobRepository
        .saveJobs(successJobsRemoved.jobs)
        .when(successJobs.nonEmpty)

      // Create new jobs that will retry the failed jobs, and submit those
      now <- ZIO.clockWith(_.instant)
      newJobs = updatedJobs.flatMap(recreateFailedJob(now, _))
      _ <- failOnTooManyRetries(newJobs)
      afterSubmitState <- submitJobs("retrying", successJobsRemoved, newJobs)
    } yield afterSubmitState
  }

  private def submitJobs(
      description: String,
      startState: EnrichedBigQueryJobs,
      newJobs: Chunk[BigQueryJob]
  ): Task[EnrichedBigQueryJobs] = {
    def saveStateAndSubmit(newState: EnrichedBigQueryJobs): ZIO[Any, Throwable, Unit] =
      for {
        _ <- jobRepository.saveJobs(newState.jobs)
        _ <- bigQueryJobService.submitJobs(newJobs)
        _ <- ZIO.succeed(submittedJobsToBqCounter.inc(newJobs.size))
      } yield ()

    if (newJobs.isEmpty) {
      ZIO.succeed(startState)
    } else {
      val afterState = startState.addCreatedJobs(newJobs)
      for {
        _ <- saveStateAndSubmit(afterState).uninterruptible
        _ <- ZIO.logInfo(
          s"Submitted ${newJobs.size} $description load jobs, " +
            s"there are now ${afterState.size} jobs."
        )
      } yield afterState
    }
  }

  private def createJobsForNewAvroFiles(
      jobsCreationTime: Instant,
      newAvroFiles: Chunk[AvroGcsFileName]
  ): Chunk[BigQueryJob] = {
    Chunk.fromIterable(
      newAvroFiles
        .groupBy(_.tableId)
        .flatMap { case (tableId, avroFiles) =>
          val avroRemoteRefs = selectCompatibleAvroFilesNew(avroFiles)
          createNewJob(
            jobsCreationTime,
            tableId,
            avroRemoteRefs
          )
        }
    )
  }

  private def createNewJob(
      jobsCreationTime: Instant,
      tableId: TableId,
      avroFileRefs: Chunk[AvroGcsFileName],
  ): Option[BigQueryJob] =
    Option.when(avroFileRefs.nonEmpty) {
      BigQueryJob(
        jobId = BQJobId.generate(config.jobPrefix, tableId),
        tableId = tableId,
        fileRefs = avroFileRefs.map(_.remoteFileRef).toVector,
        // Job Status is set to `New` because the job has not been submitted yet.
        jobStatus = JobStatus.New,
        creationTime = jobsCreationTime,
        retryCount = 0
      )
    }

  private def recreateFailedJob(
      creationTime: Instant,
      failedJob: BigQueryJob
  ): Option[BigQueryJob] =
    Option.when(failedJob.isFailed)(failedJob.retryJob(config.jobPrefix, creationTime))

  private def failOnTooLongRunningJobs(jobs: Chunk[BigQueryJob]): ZIO[Any, Throwable, Unit] =
    Clock.instant.flatMap { now =>
      val minExpectedCreationTime = now.minus(config.bigQueryJobMaxRuntime)
      val runningTooLongJobs =
        jobs.filter(job => !job.isFinal && job.creationTime < minExpectedCreationTime)

      ZIO
        .when(runningTooLongJobs.nonEmpty) {
          ZIO.logError(s"Some jobs were running too long: $runningTooLongJobs.") *>
            ZIO.fail(JobCoordinator.JobRunningTooLong)
        }
        .unit
    }

  private def failOnTooManyRetries(jobs: Chunk[BigQueryJob]): ZIO[Any, Throwable, Unit] = {
    val permanentlyFailedJobs =
      jobs.filter(_.retryCount > config.maxRetries)

    ZIO
      .when(permanentlyFailedJobs.nonEmpty) {
        ZIO.logError(s"Some jobs failed too often: $permanentlyFailedJobs") *>
          ZIO.fail(JobCoordinator.TooManyRetries)
      }
      .unit
  }

  private def observeJobCount(state: EnrichedBigQueryJobs): ZIO[Any, Throwable, Unit] =
    ZIO.succeed(jobsCountGauge.set(state.size.toDouble))

  private def remoteDirectoryRetryPolicy =
    Schedule.fibonacci(100.milli) && Schedule.recurs(config.maxRetries)
}
