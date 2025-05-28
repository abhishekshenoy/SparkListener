import org.apache.spark.scheduler._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.types._
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.collection.mutable
import scala.collection.concurrent.TrieMap
import java.util.concurrent.ConcurrentHashMap
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.util.{Try, Success, Failure}

// Data Models
case class SourceNodeDetail(
  tableName: String,
  folderName: String,
  sourceType: String, // PARQUET_FILE, ORC_FILE
  columnsProjected: List[String],
  filtersApplied: List[String],
  metricsBeforeFilter: MetricsDetail,
  metricsAfterFilter: MetricsDetail,
  partitionsScanned: List[String] = List.empty
)

case class TargetNodeDetail(
  tableName: String,
  folderName: String,
  targetType: String, // PARQUET_FILE, ORC_FILE
  metrics: MetricsDetail
)

case class MetricsDetail(
  recordsCount: Long,
  bytesProcessed: Long,
  filesCount: Long = 0,
  partitionsCount: Long = 0
)

case class ActionMetadata(
  actionId: String,
  actionType: String,
  timestamp: String,
  sourceNodes: List[SourceNodeDetail],
  targetNode: Option[TargetNodeDetail],
  executionTimeMs: Long,
  sparkJobId: Int,
  sparkStageIds: List[Int]
)

// Core Listener Implementation
class SparkETLMetadataListener extends SparkListener {
  
  // Thread-safe collections for tracking metrics across jobs/stages/tasks
  private val jobToStageMetrics = new ConcurrentHashMap[Int, mutable.Map[Int, StageMetrics]]()
  private val stageToTaskMetrics = new ConcurrentHashMap[Int, mutable.ListBuffer[TaskMetrics]]()
  private val activeJobs = new ConcurrentHashMap[Int, JobTrackingInfo]()
  private val jobToQueryExecution = new ConcurrentHashMap[Int, org.apache.spark.sql.execution.QueryExecution]()
  
  case class JobTrackingInfo(
    jobId: Int,
    startTime: Long,
    stageIds: Array[Int]
  )
  
  case class StageMetrics(
    stageId: Int,
    var inputRecords: Long = 0L,
    var inputBytes: Long = 0L,
    var outputRecords: Long = 0L,
    var outputBytes: Long = 0L,
    var filesRead: Long = 0L,
    var isCompleted: Boolean = false
  )

  // Fixed callback function declaration
  private var metadataCallback: ActionMetadata => Unit = { _ => 
    // Default empty implementation
  }
  
  def setMetadataCallback(callback: ActionMetadata => Unit): Unit = {
    metadataCallback = callback
  }

  // Hook to capture QueryExecution - this needs to be called from your application
  def captureQueryExecution(jobId: Int, queryExecution: org.apache.spark.sql.execution.QueryExecution): Unit = {
    jobToQueryExecution.put(jobId, queryExecution)
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    val jobId = jobStart.jobId
    val trackingInfo = JobTrackingInfo(jobId, jobStart.time, jobStart.stageIds)
    activeJobs.put(jobId, trackingInfo)
    
    // Initialize stage metrics for this job
    val stageMetricsMap = mutable.Map[Int, StageMetrics]()
    jobStart.stageIds.foreach { stageId =>
      stageMetricsMap.put(stageId, StageMetrics(stageId))
    }
    jobToStageMetrics.put(jobId, stageMetricsMap)
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val stageInfo = stageCompleted.stageInfo
    val stageId = stageInfo.stageId
    
    // Aggregate task metrics for this stage
    val taskMetricsList = stageToTaskMetrics.getOrDefault(stageId, mutable.ListBuffer.empty)
    
    val aggregatedMetrics = taskMetricsList.foldLeft(StageMetrics(stageId)) { (acc, taskMetrics) =>
      acc.inputRecords += taskMetrics.inputMetrics.recordsRead
      acc.inputBytes += taskMetrics.inputMetrics.bytesRead
      acc.outputRecords += taskMetrics.outputMetrics.recordsWritten
      acc.outputBytes += taskMetrics.outputMetrics.bytesWritten
      acc
    }
    aggregatedMetrics.isCompleted = true
    
    // Update stage metrics in all jobs that contain this stage
    jobToStageMetrics.asScala.foreach { case (jobId, stageMap) =>
      if (stageMap.contains(stageId)) {
        stageMap.put(stageId, aggregatedMetrics)
      }
    }
    
    // Clean up task metrics for this stage
    stageToTaskMetrics.remove(stageId)
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    if (taskEnd.taskMetrics != null) {
      val stageId = taskEnd.stageId
      val taskMetrics = taskEnd.taskMetrics
      
      stageToTaskMetrics.computeIfAbsent(stageId, _ => mutable.ListBuffer.empty) += taskMetrics
    }
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    val jobId = jobEnd.jobId
    val jobTrackingInfo = activeJobs.get(jobId)
    
    if (jobTrackingInfo != null) {
      try {
        val actionMetadata = extractActionMetadata(jobId, jobEnd.time - jobTrackingInfo.startTime)
        if (actionMetadata.nonEmpty) {
          metadataCallback(actionMetadata.get)
        }
      } catch {
        case e: Exception =>
          println(s"Error extracting metadata for job $jobId: ${e.getMessage}")
          e.printStackTrace()
      } finally {
        // Cleanup
        activeJobs.remove(jobId)
        jobToStageMetrics.remove(jobId)
        jobToQueryExecution.remove(jobId)
      }
    }
  }

  private def extractActionMetadata(jobId: Int, executionTimeMs: Long): Option[ActionMetadata] = {
    val stageMetricsMap = jobToStageMetrics.get(jobId)
    if (stageMetricsMap == null) return None

    try {
      val queryExecution = jobToQueryExecution.get(jobId)
      if (queryExecution == null) {
        println(s"No QueryExecution found for job $jobId")
        return None
      }

      val logicalPlan = queryExecution.logical
      val physicalPlan = queryExecution.executedPlan

      // Extract source and target information
      val sourceNodes = extractSourceNodes(logicalPlan, physicalPlan, stageMetricsMap)
      val targetNode = extractTargetNode(logicalPlan, physicalPlan, stageMetricsMap)

      val actionMetadata = ActionMetadata(
        actionId = s"job_${jobId}_${System.currentTimeMillis()}",
        actionType = determineActionType(logicalPlan),
        timestamp = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
        sourceNodes = sourceNodes,
        targetNode = targetNode,
        executionTimeMs = executionTimeMs,
        sparkJobId = jobId,
        sparkStageIds = stageMetricsMap.keys.toList
      )

      Some(actionMetadata)
    } catch {
      case e: Exception =>
        println(s"Error in extractActionMetadata: ${e.getMessage}")
        e.printStackTrace()
        None
    }
  }

  private def extractSourceNodes(
    logicalPlan: LogicalPlan, 
    physicalPlan: SparkPlan, 
    stageMetrics: mutable.Map[Int, StageMetrics]
  ): List[SourceNodeDetail] = {
    
    val sourceNodes = mutable.ListBuffer[SourceNodeDetail]()
    
    // Extract from logical plan - focus on LogicalRelation for file sources
    logicalPlan.foreach {
      case relation: LogicalRelation =>
        relation.relation match {
          case fsRelation: HadoopFsRelation =>
            // Check if it's Parquet or ORC
            val fileFormat = fsRelation.fileFormat
            if (isParquetOrOrc(fileFormat)) {
              val sourceNode = extractFileSourceFromLogicalRelation(relation, fsRelation, stageMetrics, logicalPlan)
              sourceNodes += sourceNode
            }
          case _ => // Skip non-file sources
        }
      case _ => // Skip other types
    }
    
    // Extract from physical plan for additional context and filters
    physicalPlan.foreach {
      case scan: FileSourceScanExec =>
        val fileFormat = scan.relation.fileFormat
        if (isParquetOrOrc(fileFormat)) {
          val sourceNode = extractFileSourceFromScan(scan, stageMetrics, logicalPlan)
          sourceNodes += sourceNode
        }
      case _ => // Skip other types
    }
    
    sourceNodes.toList.distinctBy(_.folderName)
  }

  private def extractFileSourceFromLogicalRelation(
    relation: LogicalRelation,
    fsRelation: HadoopFsRelation,
    stageMetrics: mutable.Map[Int, StageMetrics],
    logicalPlan: LogicalPlan
  ): SourceNodeDetail = {
    
    val paths = fsRelation.location.rootPaths.map(_.toString)
    val folderName = if (paths.nonEmpty) paths.head else ""
    val tableName = extractTableNameFromPath(folderName)
    val sourceType = getFileSourceType(fsRelation.fileFormat)
    
    val projectedColumns = relation.output.map(_.name).toList
    val filters = extractFiltersFromLogicalPlan(logicalPlan, relation)
    val fileCount = getFileCount(fsRelation.location.rootPaths)
    
    val totalMetrics = aggregateStageMetrics(stageMetrics)
    val metricsBeforeFilter = MetricsDetail(
      recordsCount = totalMetrics.inputRecords,
      bytesProcessed = totalMetrics.inputBytes,
      filesCount = fileCount
    )
    
    val metricsAfterFilter = calculatePostFilterMetrics(totalMetrics, filters)
    
    SourceNodeDetail(
      tableName = tableName,
      folderName = folderName,
      sourceType = sourceType,
      columnsProjected = projectedColumns,
      filtersApplied = filters,
      metricsBeforeFilter = metricsBeforeFilter,
      metricsAfterFilter = metricsAfterFilter,
      partitionsScanned = extractPartitionInfo(fsRelation)
    )
  }

  private def extractFileSourceFromScan(
    scan: FileSourceScanExec, 
    stageMetrics: mutable.Map[Int, StageMetrics],
    logicalPlan: LogicalPlan
  ): SourceNodeDetail = {
    
    val folderName = scan.relation.location.rootPaths.headOption.map(_.toString).getOrElse("")
    val tableName = extractTableNameFromPath(folderName)
    val sourceType = getFileSourceType(scan.relation.fileFormat)
    val projectedColumns = scan.output.map(_.name).toList
    
    // Extract filters from the scan - these are pushed down filters
    val pushedFilters = scan.dataFilters.map(exprToString).toList
    val partitionFilters = scan.partitionFilters.map(exprToString).toList
    val allFilters = pushedFilters ++ partitionFilters
    
    val fileCount = getFileCount(scan.relation.location.rootPaths)
    
    val totalMetrics = aggregateStageMetrics(stageMetrics)
    
    // For FileSourceScanExec, we can get more accurate metrics
    val metricsBeforeFilter = MetricsDetail(
      recordsCount = totalMetrics.inputRecords,
      bytesProcessed = totalMetrics.inputBytes,
      filesCount = fileCount
    )
    
    // After filter metrics - use output metrics from the scan stage
    val metricsAfterFilter = MetricsDetail(
      recordsCount = totalMetrics.outputRecords,
      bytesProcessed = totalMetrics.outputBytes,
      filesCount = fileCount
    )
    
    SourceNodeDetail(
      tableName = tableName,
      folderName = folderName,
      sourceType = sourceType,
      columnsProjected = projectedColumns,
      filtersApplied = allFilters,
      metricsBeforeFilter = metricsBeforeFilter,
      metricsAfterFilter = metricsAfterFilter,
      partitionsScanned = scan.selectedPartitions.map(_.toString).toList
    )
  }

  private def extractTargetNode(
    logicalPlan: LogicalPlan, 
    physicalPlan: SparkPlan, 
    stageMetrics: mutable.Map[Int, StageMetrics]
  ): Option[TargetNodeDetail] = {
    
    // Look for write operations in the logical plan
    logicalPlan.foreach {
      case command: SaveIntoDataSourceCommand =>
        val path = command.options.getOrElse("path", "")
        if (path.nonEmpty && isParquetOrOrcPath(path)) {
          val totalMetrics = aggregateStageMetrics(stageMetrics)
          val targetType = getTargetTypeFromPath(path)
          return Some(TargetNodeDetail(
            tableName = extractTableNameFromPath(path),
            folderName = path,
            targetType = targetType,
            metrics = MetricsDetail(
              recordsCount = totalMetrics.outputRecords,
              bytesProcessed = totalMetrics.outputBytes,
              filesCount = 1L // Will be determined after write
            )
          ))
        }
        
      case command: CreateDataSourceTableAsSelectCommand =>
        val table = command.table
        val location = table.storage.locationUri.map(_.toString).getOrElse("")
        if (location.nonEmpty && isParquetOrOrcPath(location)) {
          val totalMetrics = aggregateStageMetrics(stageMetrics)
          val targetType = getTargetTypeFromPath(location)
          return Some(TargetNodeDetail(
            tableName = s"${table.database}.${table.identifier.table}",
            folderName = location,
            targetType = targetType,
            metrics = MetricsDetail(
              recordsCount = totalMetrics.outputRecords,
              bytesProcessed = totalMetrics.outputBytes
            )
          ))
        }
        
      case _ => // Continue searching
    }
    
    // Check physical plan for write operations
    physicalPlan.foreach {
      case write: WriteFilesExec =>
        // Extract path from write operation
        val path = extractPathFromWriteExec(write)
        if (path.nonEmpty && isParquetOrOrcPath(path)) {
          val totalMetrics = aggregateStageMetrics(stageMetrics)
          val targetType = getTargetTypeFromPath(path)
          return Some(TargetNodeDetail(
            tableName = extractTableNameFromPath(path),
            folderName = path,
            targetType = targetType,
            metrics = MetricsDetail(
              recordsCount = totalMetrics.outputRecords,
              bytesProcessed = totalMetrics.outputBytes
            )
          ))
        }
      case _ => // Continue searching
    }
    
    None
  }

  // Helper methods with actual implementations
  private def isParquetOrOrc(fileFormat: org.apache.spark.sql.execution.datasources.FileFormat): Boolean = {
    fileFormat match {
      case _: org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat => true
      case _: org.apache.spark.sql.execution.datasources.orc.OrcFileFormat => true
      case _ => false
    }
  }

  private def isParquetOrOrcPath(path: String): Boolean = {
    val lowerPath = path.toLowerCase
    lowerPath.contains(".parquet") || lowerPath.contains(".orc") || 
    lowerPath.contains("parquet") || lowerPath.contains("orc")
  }

  private def getFileSourceType(fileFormat: org.apache.spark.sql.execution.datasources.FileFormat): String = {
    fileFormat match {
      case _: org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat => "PARQUET_FILE"
      case _: org.apache.spark.sql.execution.datasources.orc.OrcFileFormat => "ORC_FILE"
      case _ => "UNKNOWN_FILE"
    }
  }

  private def getTargetTypeFromPath(path: String): String = {
    val lowerPath = path.toLowerCase
    if (lowerPath.contains("parquet")) "PARQUET_FILE"
    else if (lowerPath.contains("orc")) "ORC_FILE"
    else "FILE_SINK"
  }

  private def extractFiltersFromLogicalPlan(logicalPlan: LogicalPlan, targetRelation: LogicalRelation): List[String] = {
    val filters = mutable.ListBuffer[String]()
    
    // Traverse the logical plan to find filters applied to this relation
    logicalPlan.foreach {
      case filter: Filter =>
        // Check if this filter is applied to our target relation
        if (isFilterAppliedToRelation(filter, targetRelation)) {
          filters += exprToString(filter.condition)
        }
      case _ => // Continue searching
    }
    
    filters.toList
  }

  private def isFilterAppliedToRelation(filter: Filter, targetRelation: LogicalRelation): Boolean = {
    // Check if the filter's child is the target relation or contains it
    def containsRelation(plan: LogicalPlan): Boolean = {
      plan match {
        case relation if relation == targetRelation => true
        case _ => plan.children.exists(containsRelation)
      }
    }
    
    containsRelation(filter.child)
  }

  private def exprToString(expr: Expression): String = {
    expr.prettyName match {
      case name if name.nonEmpty => s"$name(${expr.children.map(_.toString).mkString(", ")})"
      case _ => expr.toString
    }
  }

  private def getFileCount(rootPaths: Seq[org.apache.hadoop.fs.Path]): Long = {
    try {
      val spark = SparkSession.getActiveSession
      if (spark.isEmpty) return 1L
      
      val hadoopConf = spark.get.sessionState.newHadoopConf()
      
      rootPaths.map { path =>
        Try {
          val fs = path.getFileSystem(hadoopConf)
          val fileStatus = fs.listStatus(path)
          fileStatus.count(_.isFile).toLong
        }.getOrElse(1L)
      }.sum
    } catch {
      case _: Exception => 1L
    }
  }

  private def extractPathFromWriteExec(write: WriteFilesExec): String = {
    // Extract path from WriteFilesExec - this is version dependent
    try {
      write.toString match {
        case s if s.contains("path=") =>
          val pathStart = s.indexOf("path=") + 5
          val pathEnd = s.indexOf(",", pathStart)
          if (pathEnd > pathStart) s.substring(pathStart, pathEnd)
          else s.substring(pathStart)
        case _ => ""
      }
    } catch {
      case _: Exception => ""
    }
  }

  private def calculatePostFilterMetrics(stageMetrics: StageMetrics, filters: List[String]): MetricsDetail = {
    if (filters.isEmpty) {
      MetricsDetail(
        recordsCount = stageMetrics.inputRecords,
        bytesProcessed = stageMetrics.inputBytes,
        filesCount = stageMetrics.filesRead
      )
    } else {
      // Use output metrics as post-filter metrics when filters are present
      MetricsDetail(
        recordsCount = stageMetrics.outputRecords,
        bytesProcessed = stageMetrics.outputBytes,
        filesCount = stageMetrics.filesRead
      )
    }
  }

  private def aggregateStageMetrics(stageMetrics: mutable.Map[Int, StageMetrics]): StageMetrics = {
    stageMetrics.values.foldLeft(StageMetrics(0)) { (acc, metrics) =>
      acc.inputRecords += metrics.inputRecords
      acc.inputBytes += metrics.inputBytes
      acc.outputRecords += metrics.outputRecords
      acc.outputBytes += metrics.outputBytes
      acc.filesRead += metrics.filesRead
      acc
    }
  }

  private def extractTableNameFromPath(path: String): String = {
    if (path.nonEmpty) {
      val normalizedPath = path.replaceAll("\\\\", "/")
      val parts = normalizedPath.split("/")
      val fileName = parts.lastOption.getOrElse("unknown_table")
      // Remove file extensions if present
      fileName.replaceAll("\\.(parquet|orc)$", "")
    } else {
      "unknown_table"
    }
  }

  private def extractPartitionInfo(fsRelation: HadoopFsRelation): List[String] = {
    try {
      fsRelation.partitionSchema.fieldNames.toList
    } catch {
      case _: Exception => List.empty
    }
  }

  private def determineActionType(logicalPlan: LogicalPlan): String = {
    logicalPlan match {
      case _: Command => "WRITE_COMMAND"
      case plan if plan.find(_.isInstanceOf[org.apache.spark.sql.catalyst.plans.logical.Aggregate]).nonEmpty => "AGGREGATION"
      case plan if plan.find(_.isInstanceOf[org.apache.spark.sql.catalyst.plans.logical.Join]).nonEmpty => "JOIN"
      case _ => "READ_QUERY"
    }
  }
}

// Enhanced Usage Example with proper QueryExecution capture
object SparkETLMetadataListenerExample {
  
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession
    
    val spark = SparkSession.builder()
      .appName("ETL Metadata Listener Example")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "false") // Disable AQE for clearer execution
      .getOrCreate()
    
    // Create and register the listener
    val metadataListener = new SparkETLMetadataListener()
    
    // Set up callback to handle captured metadata
    metadataListener.setMetadataCallback { actionMetadata =>
      println("=== ACTION METADATA CAPTURED ===")
      println(s"Action ID: ${actionMetadata.actionId}")
      println(s"Action Type: ${actionMetadata.actionType}")
      println(s"Timestamp: ${actionMetadata.timestamp}")
      println(s"Execution Time: ${actionMetadata.executionTimeMs}ms")
      println(s"Job ID: ${actionMetadata.sparkJobId}")
      println(s"Stage IDs: ${actionMetadata.sparkStageIds.mkString(", ")}")
      
      println("\n--- SOURCE NODES ---")
      actionMetadata.sourceNodes.foreach { source =>
        println(s"Table: ${source.tableName}")
        println(s"Folder: ${source.folderName}")
        println(s"Type: ${source.sourceType}")
        println(s"Columns: ${source.columnsProjected.mkString(", ")}")
        println(s"Filters: ${source.filtersApplied.mkString("; ")}")
        println(s"Before Filter - Records: ${source.metricsBeforeFilter.recordsCount}, Bytes: ${source.metricsBeforeFilter.bytesProcessed}, Files: ${source.metricsBeforeFilter.filesCount}")
        println(s"After Filter - Records: ${source.metricsAfterFilter.recordsCount}, Bytes: ${source.metricsAfterFilter.bytesProcessed}")
        if (source.partitionsScanned.nonEmpty) {
          println(s"Partitions: ${source.partitionsScanned.mkString(", ")}")
        }
        println()
      }
      
      println("--- TARGET NODE ---")
      actionMetadata.targetNode.foreach { target =>
        println(s"Table: ${target.tableName}")
        println(s"Folder: ${target.folderName}")
        println(s"Type: ${target.targetType}")
        println(s"Records: ${target.metrics.recordsCount}, Bytes: ${target.metrics.bytesProcessed}")
      }
      
      println("=== END METADATA ===\n")
    }
    
    // Register the listener
    spark.sparkContext.addSparkListener(metadataListener)
    
    // Example ETL operations with QueryExecution capture
    import spark.implicits._
    
    try {
      // Create sample data for testing
      val sampleData = (1 to 1000).map(i => (i, s"name_$i", 20 + (i % 50), if (i % 2 == 0) "active" else "inactive"))
      val df = sampleData.toDF("id", "name", "age", "status")
      
      // Write sample data to parquet
      df.write.mode("overwrite").parquet("/tmp/test_input.parquet")
      
      // Read from parquet file source
      val inputDf = spark.read.parquet("/tmp/test_input.parquet")
      
      // Capture QueryExecution for the read operation
      val readQuery = inputDf.queryExecution
      
      // Apply transformations with filters
      val filteredDf = inputDf
        .filter($"age" > 25)
        .filter($"status" === "active")
        .select("id", "name", "age")
      
      // Capture QueryExecution for the filtered operation
      val filterQuery = filteredDf.queryExecution
      
      // Action that triggers the listener - collect
      spark.sparkContext.setJobDescription("Collect filtered data")
      metadataListener.captureQueryExecution(spark.sparkContext.getNextJobId, filterQuery)
      val collectResult = filteredDf.collect()
      println(s"Collected ${collectResult.length} records")
      
      // Write operation
      spark.sparkContext.setJobDescription("Write to parquet")
      val writeQuery = filteredDf.write.mode("overwrite").parquet("/tmp/test_output.parquet")
      // Note: For write operations, capturing QueryExecution is more complex and may require custom hooks
      
    } catch {
      case e: Exception =>
        println(s"Error in example: ${e.getMessage}")
        e.printStackTrace()
    }
    
    // Allow time for async operations to complete
    Thread.sleep(2000)
    
    spark.stop()
  }
}