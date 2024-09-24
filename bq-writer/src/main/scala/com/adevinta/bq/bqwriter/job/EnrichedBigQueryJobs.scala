package com.adevinta.bq.bqwriter.job

import com.adevinta.bq.shared.remotedir.RemoteFileRef
import com.adevinta.bq.shared.remotedir.TableId
import zio._

import java.time.Instant

final case class EnrichedBigQueryJobs private[bqwriter] (
    jobsMap: Map[TableId, BigQueryJob],
) {
  lazy val jobs: Chunk[BigQueryJob] = Chunk.fromIterable(jobsMap.values)

  def addCreatedJobs(createdJobs: Chunk[BigQueryJob]): EnrichedBigQueryJobs =
    copy(jobsMap = jobsMap ++ createdJobs.map(j => j.tableId -> j))

  def size: Int = jobsMap.size

  def jobsRemoved(jobs: Seq[BigQueryJob]): EnrichedBigQueryJobs =
    copy(jobsMap = jobsMap -- jobs.map(_.tableId))

  def avroFilesInActiveLoadJobFileRefs: Set[RemoteFileRef] =
    jobsMap.values.flatMap(_.fileRefs).toSet

  /** Returns the creation time of the oldest job, or `None` when there a no jobs.
    *
    * For any job `j`, following invariant holds: {{{j.creationTime >= creationTimeOfOldestJob}}}
    */
  def creationTimeOfOldestJob: Option[Instant] =
    jobsMap.values.map(_.creationTime).minOption

  def tableIdsInActiveLoadJobs: Set[TableId] = jobsMap.keySet
}

object EnrichedBigQueryJobs {
  def make(jobs: Chunk[BigQueryJob]): EnrichedBigQueryJobs = {
    // Table ID is used as the key in the jobsMap of EnrichedBigQueryJobs
    // There can be only one instance of BigQueryJob per table Id active at any given time
    val jobsByTableId = jobs.map(job => job.tableId -> job).toMap
    require(jobs.size == jobsByTableId.size, "Duplicate table ids found in jobs")
    EnrichedBigQueryJobs(jobsByTableId)
  }

  val empty: EnrichedBigQueryJobs =
    new EnrichedBigQueryJobs(Map.empty)

}
