import sys.process._

// This scripts needs to be executed from your local machine, after the login to gcp with `gcloud auth login`
// with `scala removeDuplicates.sc`

// PARAMETERS
// datasetsToBeDeduplicated: the sequence of datasets that needs to be deduplicated
// projectId: projectId in which the datasets are located
// daysToDeduplicate: days in the past in which we want to apply deduplication
// note: setting a custom date in the query will make the deduplication happen only for that date

/*
 WARNING:
 This script removes duplicate rows based on the 'event.eventId' field. Although 'event.eventId' isn't the primary key in BigQuery,
 it ideally should be unique for each event. Before executing this script, it's advisable to contact the data producers to clarify
 the specific purpose for which they are using 'event.eventId'.
*/

val datasetsToBeDeduplicated = Seq("dataset-name")

val projectId = "gcp-project-id"

val daysToDeduplicate = 5

val tableIdsToBeDeduplicated = datasetsToBeDeduplicated.flatMap { datasetName =>
  val query =
    s"""
       |SELECT table_name
       |FROM `$projectId.$datasetName`.INFORMATION_SCHEMA.TABLES;
       |""".stripMargin

  val gcpCommands = s"bq query --use_legacy_sql=false '$query'"

  val queryResult = gcpCommands.!!

  queryResult
    .split('\n')
    .toList
    .filter(_.startsWith("| "))
    .tail
    .flatMap(_.split(" ").toList.filterNot(elem => elem.contains("|") || elem.isEmpty))
    .map(projectId + "." + datasetName + "." + _)

}

println(tableIdsToBeDeduplicated.mkString("\n"))

tableIdsToBeDeduplicated.foreach { tableId =>

  val query =
    s"""
       |MERGE `$tableId`
       |  USING (
       |    WITH ranked_data AS (
       |      SELECT *,
       |      ROW_NUMBER() OVER (PARTITION BY event.eventId) AS rn
       |        FROM `$tableId`
       |        WHERE eventDate between DATE_SUB(current_date(), INTERVAL $daysToDeduplicate DAY) AND CURRENT_DATE()
       |    )
       |      SELECT * except(rn)
       |      FROM ranked_data
       |      WHERE rn = 1 AND
       |      eventDate between DATE_SUB(current_date(), INTERVAL $daysToDeduplicate DAY) AND CURRENT_DATE()
       |)
       |ON FALSE
       |  WHEN NOT MATCHED BY SOURCE AND eventDate between DATE_SUB(current_date(), INTERVAL $daysToDeduplicate DAY) AND CURRENT_DATE() THEN DELETE
       |WHEN NOT MATCHED BY TARGET AND eventDate between DATE_SUB(current_date(), INTERVAL $daysToDeduplicate DAY) AND CURRENT_DATE() THEN INSERT ROW
       |""".stripMargin

  val gcpCommands = s"bq query --use_legacy_sql=false '$query'"

  println(tableId)
  gcpCommands.!!

}
