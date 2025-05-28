import org.apache.spark.scheduler._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.v2._ // For Spark 3.x V2 sources
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.command.InsertIntoHadoopFsRelationCommand
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Case class to hold various metric values.
 * @param filesRead Number of files read.
 * @param recordsRead Number of records read.
 * @param dataSizeScannedBytes Data size scanned in bytes.
 * @param recordsWritten Number of records written.
 * @param dataSizeWrittenBytes Data size written in bytes.
 */
case class Metrics(
    filesRead: Long = 0,
    recordsRead: Long = 0,
    dataSizeScannedBytes: Long = 0,
    recordsWritten: Long = 0,
    dataSizeWrittenBytes: Long = 0
) {
    /** Provides a human-readable string representation of the metrics. */
    override def toString: String =
        s"(Files: $filesRead, Records: $recordsRead, Size: ${dataSizeScannedBytes / (1024 * 1024.0)} MB, Written Records: $recordsWritten, Written Size: ${dataSizeWrittenBytes / (1024 * 1024.0)} MB)"
}

/**
 * Case class to detail a source node in the ETL workflow.
 * @param tableNameOrFolderPath The path or name of the source (e.g., file path).
 * @param columnsProjected The names of columns projected from this source.
 * @param filtersApplied The SQL expressions of filters applied to this source.
 * @param metricsBeforeFilter Metrics captured before any filter operations.
 * @param metricsAfterFilter Metrics captured after filter operations.
 */
case class SourceNodeDetail(
    tableNameOrFolderPath: String,
    columnsProjected: Seq[String],
    filtersApplied: Seq[String],
    metricsBeforeFilter: Metrics,
    metricsAfterFilter: Metrics
) {
    /** Provides a human-readable string representation of the source node details. */
    override def toString: String =
        s"""
           |  Path: $tableNameOrFolderPath
           |  Columns: ${columnsProjected.mkString(", ")}
           |  Filters: ${if (filtersApplied.nonEmpty) filtersApplied.mkString(" AND ") else "None"}
           |  Metrics Before Filter: $metricsBeforeFilter
           |  Metrics After Filter: $metricsAfterFilter
         """.stripMargin
}

/**
 * Case class to detail a target node in the ETL workflow.
 * @param tableNameOrFolderPath The path or name of the target (e.g., output file path).
 * @param metrics Metrics captured for the write operation.
 */
case class TargetNodeDetail(
    tableNameOrFolderPath: String,
    metrics: Metrics
) {
    /** Provides a human-readable string representation of the target node details. */
    override def toString: String =
        s"""
           |  Path: $tableNameOrFolderPath
           |  Metrics: $metrics
         """.stripMargin
}

/**
 * Case class to encapsulate all metadata captured for a single Spark action.
 * @param jobId The Spark job ID.
 * @param actionName A descriptive name for the Spark action.
 * @param sourceNodes A sequence of source node details.
 * @param targetNode An optional target node detail.
 */
case class ActionMetadata(
    jobId: Int,
    actionName: String,
    sourceNodes: Seq[SourceNodeDetail],
    targetNode: Option[TargetNodeDetail]
)

/**
 * A custom SparkListener to capture ETL workflow metadata and metrics.
 * This listener focuses on file source reads (Parquet/ORC), filter operations,
 * and associated metrics before and after filtering, as well as target write metrics.
 *
 * It leverages Spark's internal SQL execution events and physical plan traversal
 * to extract the required information.
 */
class ETLWorkflowListener extends SparkListener {

    // Maps Spark Job ID to Query Execution ID. A job can trigger one or more SQL executions.
    private val jobExecutionIdMap = mutable.Map[Int, Long]()
    // Stores the physical plan (SparkPlan) associated with a Query Execution ID.
    private val executionPlanMap = mutable.Map[Long, SparkPlan]()
    // Stores the accumulated SQL metric values for each Query Execution ID.
    // The inner map is (Metric ID -> Metric Value).
    private val sqlMetricsMap = mutable.Map[Long, Map[Long, Long]]()

    /**
     * Called when a Spark job starts.
     * Currently, no specific action is taken here for this use case.
     */
    override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
        // No action needed for this specific use case, but can be used for initial tracking.
    }

    /**
     * Called when a Spark job ends. This is the primary entry point for extracting and
     * reporting the captured metadata and metrics.
     */
    override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
        val jobId = jobEnd.jobId
        // Get the action name from job properties, defaulting to "Job X" if not found.
        val actionName = jobEnd.properties.getProperty("spark.job.description", s"Job $jobId")

        // Retrieve the associated Query Execution ID and then the physical plan.
        jobExecutionIdMap.get(jobId).foreach { executionId =>
            executionPlanMap.get(executionId).foreach { physicalPlan =>
                // Get the recorded SQL metrics for this execution.
                val recordedMetrics = sqlMetricsMap.getOrElse(executionId, Map.empty)

                // Extract source and target node details from the physical plan.
                val sourceNodes = extractSourceNodeDetails(physicalPlan, recordedMetrics)
                val targetNode = extractTargetNodeDetails(physicalPlan, recordedMetrics)

                // Assemble and print the captured metadata.
                val metadata = ActionMetadata(jobId, actionName, sourceNodes, targetNode)
                println(s"\n--- Captured Metadata for Job $jobId ($actionName) ---")
                println(s"Source Nodes: ${if (metadata.sourceNodes.nonEmpty) metadata.sourceNodes.mkString("\n") else "None"}")
                println(s"Target Node: ${metadata.targetNode.getOrElse("N/A")}")
                println("-------------------------------------------------\n")
            }
        }

        // Clean up internal maps to prevent memory leaks for long-running applications.
        // It's important to remove entries once they are processed.
        jobExecutionIdMap.remove(jobId)
        // Remove associated execution plan and metrics if they exist.
        jobExecutionIdMap.get(jobId).foreach { execId =>
            executionPlanMap.remove(execId)
            sqlMetricsMap.remove(execId)
        }
    }

    /**
     * Called for other Spark events not covered by specific `on*` methods.
     * This is used to capture `SparkListenerSQLExecutionStart` and `SparkListenerSQLExecutionEnd`
     * events, which are crucial for accessing physical plans and SQL metrics.
     */
    override def onOtherEvent(event: SparkListenerEvent): Unit = {
        event match {
            case sqle: SparkListenerSQLExecutionStart =>
                // When a SQL execution starts, store its physical plan.
                executionPlanMap(sqle.executionId) = sqle.sparkPlan
                // Map the Spark Job ID (if available in properties) to this execution ID.
                val jobIdStr = sqle.properties.getProperty("spark.job.id")
                if (jobIdStr != null) {
                    jobExecutionIdMap(jobIdStr.toInt) = sqle.executionId
                }

            case sqle: SparkListenerSQLExecutionEnd =>
                // When a SQL execution ends, capture the final accumulated SQL metrics.
                // The `metrics` map contains (Metric ID -> Metric Value).
                sqlMetricsMap(sqle.executionId) = sqle.metrics.mapValues(_.value).toMap

            case _ => // Ignore other events that are not relevant for this listener.
        }
    }

    /**
     * Extracts source node details by traversing the physical plan.
     * It identifies `FileSourceScanExec` nodes and their associated `FilterExec` parents.
     *
     * @param plan The physical plan to traverse.
     * @param recordedMetrics A map of recorded SQL metrics (Metric ID -> Value).
     * @return A sequence of `SourceNodeDetail` objects.
     */
    private def extractSourceNodeDetails(
        plan: SparkPlan,
        recordedMetrics: Map[Long, Long]
    ): Seq[SourceNodeDetail] = {
        val foundSourceNodes = mutable.ArrayBuffer[SourceNodeDetail]()
        // Use `foreachUp` to traverse the plan from leaves to root, which helps in
        // identifying parent-child relationships like FilterExec -> FileSourceScanExec.
        plan.foreachUp {
            case filterExec: FilterExec =>
                // If the current node is a FilterExec, check its children.
                filterExec.children.foreach {
                    case fs: FileSourceScanExec =>
                        // If a FileSourceScanExec is a direct child of a FilterExec,
                        // this means the filter is applied directly on the source.
                        val (path, format) = getFileSourceInfo(fs)
                        // Only process Parquet or ORC file sources as per requirement.
                        if (path.nonEmpty && (format.equalsIgnoreCase("Parquet") || format.equalsIgnoreCase("ORC"))) {
                            val columns = fs.output.map(_.name) // Columns projected by the scan.
                            val filters = Seq(filterExec.condition.sql) // The SQL expression of the filter.
                            val metricsBefore = getMetricsFromFileSourceScan(fs, recordedMetrics)
                            val metricsAfter = getMetricsFromFilterExec(filterExec, recordedMetrics)
                            foundSourceNodes += SourceNodeDetail(path, columns, filters, metricsBefore, metricsAfter)
                        }
                    case _ => // Child is not a FileSourceScanExec, continue traversal.
                }
            case fs: FileSourceScanExec =>
                val (path, format) = getFileSourceInfo(fs)
                // Check if this FileSourceScanExec was already captured as part of a FilterExec
                // to avoid duplicates if a source is part of a filtered branch.
                val alreadyCaptured = foundSourceNodes.exists(_.tableNameOrFolderPath == path)
                if (!alreadyCaptured && path.nonEmpty && (format.equalsIgnoreCase("Parquet") || format.equalsIgnoreCase("ORC"))) {
                    // If a FileSourceScanExec is found without a direct FilterExec parent,
                    // then the "after filter" metrics are the same as "before filter".
                    val columns = fs.output.map(_.name)
                    val metrics = getMetricsFromFileSourceScan(fs, recordedMetrics)
                    foundSourceNodes += SourceNodeDetail(path, columns, Seq.empty, metrics, metrics) // No filter applied directly above.
                }
            case _ => // Ignore other plan nodes that are not relevant for source extraction.
        }
        // Use distinct to handle cases where the same source might appear multiple times in a complex plan.
        foundSourceNodes.toSeq.distinct
    }

    /**
     * Extracts target node details by looking for write operations in the physical plan.
     * It specifically looks for `WriteFilesExec` (for V2 writes) and `DataWritingCommandExec`
     * (for commands like `InsertIntoHadoopFsRelationCommand`).
     *
     * @param plan The physical plan to traverse.
     * @param recordedMetrics A map of recorded SQL metrics (Metric ID -> Value).
     * @return An `Option` containing `TargetNodeDetail` if a target is found, otherwise `None`.
     */
    private def extractTargetNodeDetails(
        plan: SparkPlan,
        recordedMetrics: Map[Long, Long]
    ): Option[TargetNodeDetail] = {
        plan.collectFirst {
            case w: WriteFilesExec => // Handles direct DataFrame writes (e.g., df.write.format(...).save())
                // The file format string can serve as a path placeholder or be refined.
                val outputPath = w.fileFormat.toString
                val metrics = getMetricsFromWriteFiles(w, recordedMetrics)
                Some(TargetNodeDetail(outputPath, metrics))
            case dwc: DataWritingCommandExec => // Handles commands like INSERT INTO, CREATE TABLE AS SELECT
                dwc.cmd match {
                    case i: InsertIntoHadoopFsRelationCommand =>
                        // Extract the output path from the InsertIntoHadoopFsRelationCommand.
                        val outputPath = i.outputPath.toString
                        // Metrics for write operations are typically on the DataWritingCommandExec itself.
                        val recordsWritten = dwc.metrics.get("numOutputRows").flatMap(m => recordedMetrics.get(m.id)).getOrElse(0L)
                        // Look for common metric names for data size written.
                        val dataSizeWritten = dwc.metrics.get("dataSize").flatMap(m => recordedMetrics.get(m.id))
                            .orElse(dwc.metrics.get("bytesWritten").flatMap(m => recordedMetrics.get(m.id)))
                            .getOrElse(0L)
                        Some(TargetNodeDetail(outputPath, Metrics(recordsWritten = recordsWritten, dataSizeWrittenBytes = dataSizeWritten)))
                    case _ => None // Ignore other types of commands.
                }
            case _ => None // Ignore other plan nodes.
        }.flatten // Flatten the Option[Option[TargetNodeDetail]] to Option[TargetNodeDetail].
    }

    /**
     * Helper function to extract the file path and format name from a `FileSourceScanExec`.
     * @param fs The `FileSourceScanExec` instance.
     * @return A tuple containing the concatenated paths and the file format name.
     */
    private def getFileSourceInfo(fs: FileSourceScanExec): (String, String) = {
        val paths = fs.relation.location.paths.mkString(",")
        // Use pattern matching to get the actual file format name.
        val formatName = fs.relation.fileFormat match {
            case _: ParquetFileFormat => "Parquet"
            case _: OrcFileFormat => "ORC"
            // Add other file formats if needed in the future.
            case other => other.getClass.getSimpleName.replace("$", "") // Fallback for other formats.
        }
        (paths, formatName)
    }

    /**
     * Retrieves metrics for a `FileSourceScanExec` node.
     * @param fs The `FileSourceScanExec` instance.
     * @param recordedMetrics A map of recorded SQL metrics (Metric ID -> Value).
     * @return A `Metrics` object containing files read, records read, and data size scanned.
     */
    private def getMetricsFromFileSourceScan(
        fs: FileSourceScanExec,
        recordedMetrics: Map[Long, Long]
    ): Metrics = {
        // Safely retrieve metric values using flatMap and getOrElse.
        val numFiles = fs.metrics.get("numFiles").flatMap(m => recordedMetrics.get(m.id)).getOrElse(0L)
        val numOutputRows = fs.metrics.get("numOutputRows").flatMap(m => recordedMetrics.get(m.id)).getOrElse(0L)
        val bytesRead = fs.metrics.get("bytesRead").flatMap(m => recordedMetrics.get(m.id)).getOrElse(0L)
        Metrics(filesRead = numFiles, recordsRead = numOutputRows, dataSizeScannedBytes = bytesRead)
    }

    /**
     * Retrieves metrics for a `FilterExec` node.
     * @param f The `FilterExec` instance.
     * @param recordedMetrics A map of recorded SQL metrics (Metric ID -> Value).
     * @return A `Metrics` object containing records read (after filtering).
     */
    private def getMetricsFromFilterExec(
        f: FilterExec,
        recordedMetrics: Map[Long, Long]
    ): Metrics = {
        // The `numOutputRows` metric on `FilterExec` gives the number of rows *after* filtering.
        val numOutputRows = f.metrics.get("numOutputRows").flatMap(m => recordedMetrics.get(m.id)).getOrElse(0L)
        Metrics(recordsRead = numOutputRows)
    }

    /**
     * Retrieves metrics for a `WriteFilesExec` node (for V2 writes).
     * @param w The `WriteFilesExec` instance.
     * @param recordedMetrics A map of recorded SQL metrics (Metric ID -> Value).
     * @return A `Metrics` object containing records written and data size written.
     */
    private def getMetricsFromWriteFiles(
        w: WriteFilesExec,
        recordedMetrics: Map[Long, Long]
    ): Metrics = {
        // Common metrics for write operations.
        val numOutputRows = w.metrics.get("numOutputRows").flatMap(m => recordedMetrics.get(m.id)).getOrElse(0L)
        // Check for common metric names for data size written.
        val dataSize = w.metrics.get("dataSize").flatMap(m => recordedMetrics.get(m.id))
            .orElse(w.metrics.get("bytesWritten").flatMap(m => recordedMetrics.get(m.id)))
            .getOrElse(0L)
        Metrics(recordsWritten = numOutputRows, dataSizeWrittenBytes = dataSize)
    }
}
