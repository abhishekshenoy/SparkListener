import org.apache.spark.scheduler._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.v2._
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.types._
import org.apache.spark.util.AccumulatorV2
import scala.collection.mutable
import scala.collection.concurrent.TrieMap
import java.util.concurrent.ConcurrentHashMap
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

// Data Models
case class SourceNodeDetail(
  tableName: String,
  folderName: String,
  sourceType: String, // FILE, HIVE_TABLE, etc.
  columnsProjected: List[String],
  filtersApplied: List[String],
  metricsBeforeFilter: MetricsDetail,
  metricsAfterFilter: MetricsDetail,
  partitionsScanned: List[String] = List.empty
)

case class TargetNodeDetail(
  tableName: String,
  folderName: String,
  targetType: String,
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
  private val jobToActionMetadata = new ConcurrentHashMap[Int, ActionMetadata]()
  private val activeJobs = new ConcurrentHashMap[Int, JobTrackingInfo]()
  
  case class JobTrackingInfo(
    jobId: Int,
    startTime: Long,
    queryExecution: Option[Any] = None,
    sqlContext: Option[Any] = None
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

  // Callback function to handle captured metadata
  private var metadataCallback: ActionMetadata => Unit = _ => {}
  
  def setMetadataCallback(callback: ActionMetadata => Unit): Unit = {
    metadataCallback = callback
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    val jobId = jobStart.jobId
    val trackingInfo = JobTrackingInfo(jobId, jobStart.time)
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
      } finally {
        // Cleanup
        activeJobs.remove(jobId)
        jobToStageMetrics.remove(jobId)
      }
    }
  }

  private def extractActionMetadata(jobId: Int, executionTimeMs: Long): Option[ActionMetadata] = {
    val stageMetricsMap = jobToStageMetrics.get(jobId)
    if (stageMetricsMap == null) return None

    try {
      // Get the SparkSession to access query execution plans
      val spark = org.apache.spark.sql.SparkSession.getActiveSession
      if (spark.isEmpty) return None

      val sparkSession = spark.get
      val queryExecution = getLastQueryExecution(sparkSession)
      
      if (queryExecution.isEmpty) return None

      val logicalPlan = queryExecution.get.logical
      val physicalPlan = queryExecution.get.executedPlan

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
        None
    }
  }

  private def getLastQueryExecution(sparkSession: org.apache.spark.sql.SparkSession): Option[org.apache.spark.sql.execution.QueryExecution] = {
    try {
      // Access the last query execution from SparkSession
      val lastQueryExecution = sparkSession.sessionState.executePlan(sparkSession.sql("SELECT 1").queryExecution.logical)
      Some(lastQueryExecution)
    } catch {
      case _: Exception => None
    }
  }

  private def extractSourceNodes(
    logicalPlan: LogicalPlan, 
    physicalPlan: SparkPlan, 
    stageMetrics: mutable.Map[Int, StageMetrics]
  ): List[SourceNodeDetail] = {
    
    val sourceNodes = mutable.ListBuffer[SourceNodeDetail]()
    
    // Extract from logical plan
    logicalPlan.foreach {
      case relation: HiveTableRelation =>
        val sourceNode = extractHiveTableSource(relation, stageMetrics)
        sourceNodes += sourceNode
        
      case relation: LogicalRelation =>
        val sourceNode = extractFileSource(relation, stageMetrics)
        sourceNodes += sourceNode
        
      case _ => // Skip other types
    }
    
    // Extract from physical plan for additional context
    physicalPlan.foreach {
      case scan: FileSourceScanExec =>
        val sourceNode = extractFileSourceFromScan(scan, stageMetrics, logicalPlan)
        sourceNodes += sourceNode
        
      case scan: BatchScanExec =>
        val sourceNode = extractBatchScanSource(scan, stageMetrics)
        sourceNodes += sourceNode
        
      case _ => // Skip other types
    }
    
    sourceNodes.toList.distinct
  }

  private def extractHiveTableSource(
    relation: HiveTableRelation, 
    stageMetrics: mutable.Map[Int, StageMetrics]
  ): SourceNodeDetail = {
    
    val tableName = s"${relation.tableMeta.database}.${relation.tableMeta.identifier.table}"
    val folderName = relation.tableMeta.storage.locationUri.map(_.toString).getOrElse("")
    
    // Get projected columns
    val projectedColumns = relation.output.map(_.name).toList
    
    // Extract filters (this is complex and requires traversing the plan)
    val filters = extractFiltersFromRelation(relation)
    
    // Calculate metrics
    val totalMetrics = aggregateStageMetrics(stageMetrics)
    val metricsBeforeFilter = MetricsDetail(
      recordsCount = totalMetrics.inputRecords,
      bytesProcessed = totalMetrics.inputBytes,
      filesCount = getFileCount(relation.tableMeta.storage.locationUri)
    )
    
    // For after filter metrics, we need to estimate based on selectivity
    val metricsAfterFilter = estimatePostFilterMetrics(metricsBeforeFilter, filters)
    
    SourceNodeDetail(
      tableName = tableName,
      folderName = folderName,
      sourceType = "HIVE_TABLE",
      columnsProjected = projectedColumns,
      filtersApplied = filters,
      metricsBeforeFilter = metricsBeforeFilter,
      metricsAfterFilter = metricsAfterFilter,
      partitionsScanned = extractPartitionsScanned(relation.tableMeta)
    )
  }

  private def extractFileSource(
    relation: LogicalRelation, 
    stageMetrics: mutable.Map[Int, StageMetrics]
  ): SourceNodeDetail = {
    
    val (tableName, folderName, sourceType, fileCount) = relation.relation match {
      case fsRelation: HadoopFsRelation =>
        val paths = fsRelation.location.rootPaths.map(_.toString)
        val folderName = if (paths.nonEmpty) paths.head else ""
        val tableName = extractTableNameFromPath(folderName)
        val fileCount = fsRelation.location.listFiles(Nil, Nil).length.toLong
        (tableName, folderName, "FILE_SOURCE", fileCount)
        
      case _ =>
        ("unknown_table", "unknown_path", "UNKNOWN_SOURCE", 0L)
    }
    
    val projectedColumns = relation.output.map(_.name).toList
    val filters = extractFiltersFromLogicalRelation(relation)
    
    val totalMetrics = aggregateStageMetrics(stageMetrics)
    val metricsBeforeFilter = MetricsDetail(
      recordsCount = totalMetrics.inputRecords,
      bytesProcessed = totalMetrics.inputBytes,
      filesCount = fileCount
    )
    
    val metricsAfterFilter = estimatePostFilterMetrics(metricsBeforeFilter, filters)
    
    SourceNodeDetail(
      tableName = tableName,
      folderName = folderName,
      sourceType = sourceType,
      columnsProjected = projectedColumns,
      filtersApplied = filters,
      metricsBeforeFilter = metricsBeforeFilter,
      metricsAfterFilter = metricsAfterFilter
    )
  }

  private def extractFileSourceFromScan(
    scan: FileSourceScanExec, 
    stageMetrics: mutable.Map[Int, StageMetrics],
    logicalPlan: LogicalPlan
  ): SourceNodeDetail = {
    
    val folderName = scan.relation.location.rootPaths.headOption.map(_.toString).getOrElse("")
    val tableName = extractTableNameFromPath(folderName)
    val projectedColumns = scan.output.map(_.name).toList
    val filters = scan.dataFilters.map(_.toString).toList
    val fileCount = scan.relation.location.listFiles(Nil, Nil).length.toLong
    
    val totalMetrics = aggregateStageMetrics(stageMetrics)
    val metricsBeforeFilter = MetricsDetail(
      recordsCount = totalMetrics.inputRecords,
      bytesProcessed = totalMetrics.inputBytes,
      filesCount = fileCount
    )
    
    val metricsAfterFilter = MetricsDetail(
      recordsCount = totalMetrics.outputRecords,
      bytesProcessed = totalMetrics.outputBytes,
      filesCount = fileCount
    )
    
    SourceNodeDetail(
      tableName = tableName,
      folderName = folderName,
      sourceType = "FILE_SOURCE",
      columnsProjected = projectedColumns,
      filtersApplied = filters,
      metricsBeforeFilter = metricsBeforeFilter,
      metricsAfterFilter = metricsAfterFilter,
      partitionsScanned = scan.selectedPartitions.map(_.toString).toList
    )
  }

  private def extractBatchScanSource(
    scan: BatchScanExec, 
    stageMetrics: mutable.Map[Int, StageMetrics]
  ): SourceNodeDetail = {
    
    val tableName = scan.scan.description()
    val projectedColumns = scan.output.map(_.name).toList
    
    val totalMetrics = aggregateStageMetrics(stageMetrics)
    val metricsBeforeFilter = MetricsDetail(
      recordsCount = totalMetrics.inputRecords,
      bytesProcessed = totalMetrics.inputBytes
    )
    
    SourceNodeDetail(
      tableName = tableName,
      folderName = "",
      sourceType = "BATCH_SOURCE",
      columnsProjected = projectedColumns,
      filtersApplied = List.empty,
      metricsBeforeFilter = metricsBeforeFilter,
      metricsAfterFilter = metricsBeforeFilter
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
        val totalMetrics = aggregateStageMetrics(stageMetrics)
        return Some(TargetNodeDetail(
          tableName = command.options.getOrElse("path", "unknown_target"),
          folderName = command.options.getOrElse("path", ""),
          targetType = "DATA_SOURCE",
          metrics = MetricsDetail(
            recordsCount = totalMetrics.outputRecords,
            bytesProcessed = totalMetrics.outputBytes
          )
        ))
        
      case command: InsertIntoHiveTable =>
        val tableName = s"${command.table.database}.${command.table.identifier.table}"
        val totalMetrics = aggregateStageMetrics(stageMetrics)
        return Some(TargetNodeDetail(
          tableName = tableName,
          folderName = command.table.storage.locationUri.map(_.toString).getOrElse(""),
          targetType = "HIVE_TABLE",
          metrics = MetricsDetail(
            recordsCount = totalMetrics.outputRecords,
            bytesProcessed = totalMetrics.outputBytes
          )
        ))
        
      case _ => // Continue searching
    }
    
    None
  }

  // Helper methods
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

  private def extractFiltersFromRelation(relation: HiveTableRelation): List[String] = {
    // This would need to be implemented based on how filters are pushed down
    // For now, returning empty list
    List.empty
  }

  private def extractFiltersFromLogicalRelation(relation: LogicalRelation): List[String] = {
    // Extract filters from the relation if available
    List.empty
  }

  private def estimatePostFilterMetrics(beforeFilter: MetricsDetail, filters: List[String]): MetricsDetail = {
    if (filters.isEmpty) {
      beforeFilter
    } else {
      // Simple estimation - assume 50% selectivity for filters
      // In practice, you might want to use Spark's statistics or implement more sophisticated estimation
      val selectivity = 0.5
      MetricsDetail(
        recordsCount = (beforeFilter.recordsCount * selectivity).toLong,
        bytesProcessed = (beforeFilter.bytesProcessed * selectivity).toLong,
        filesCount = beforeFilter.filesCount
      )
    }
  }

  private def extractTableNameFromPath(path: String): String = {
    if (path.nonEmpty) {
      val parts = path.split("/")
      parts.lastOption.getOrElse("unknown_table")
    } else {
      "unknown_table"
    }
  }

  private def getFileCount(locationUri: Option[java.net.URI]): Long = {
    // Implementation to count files in the location
    // This is a placeholder - actual implementation would depend on your file system
    1L
  }

  private def extractPartitionsScanned(tableMeta: CatalogTable): List[String] = {
    // Extract partition information from table metadata
    tableMeta.partitionColumnNames.toList
  }

  private def determineActionType(logicalPlan: LogicalPlan): String = {
    logicalPlan match {
      case _: Command => "COMMAND"
      case plan if plan.find(_.isInstanceOf[org.apache.spark.sql.catalyst.plans.logical.Aggregate]).nonEmpty => "AGGREGATION"
      case plan if plan.find(_.isInstanceOf[org.apache.spark.sql.catalyst.plans.logical.Join]).nonEmpty => "JOIN"
      case _ => "QUERY"
    }
  }
}

// Usage Example and Integration
object SparkETLMetadataListenerExample {
  
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession
    
    val spark = SparkSession.builder()
      .appName("ETL Metadata Listener Example")
      .master("local[*]")
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
      
      println("\n--- SOURCE NODES ---")
      actionMetadata.sourceNodes.foreach { source =>
        println(s"Table: ${source.tableName}")
        println(s"Folder: ${source.folderName}")
        println(s"Type: ${source.sourceType}")
        println(s"Columns: ${source.columnsProjected.mkString(", ")}")
        println(s"Filters: ${source.filtersApplied.mkString(", ")}")
        println(s"Before Filter - Records: ${source.metricsBeforeFilter.recordsCount}, Bytes: ${source.metricsBeforeFilter.bytesProcessed}")
        println(s"After Filter - Records: ${source.metricsAfterFilter.recordsCount}, Bytes: ${source.metricsAfterFilter.bytesProcessed}")
        println()
      }
      
      println("--- TARGET NODE ---")
      actionMetadata.targetNode.foreach { target =>
        println(s"Table: ${target.tableName}")
        println(s"Type: ${target.targetType}")
        println(s"Records: ${target.metrics.recordsCount}, Bytes: ${target.metrics.bytesProcessed}")
      }
      
      println("=== END METADATA ===\n")
    }
    
    // Register the listener
    spark.sparkContext.addSparkListener(metadataListener)
    
    // Example ETL operations
    import spark.implicits._
    
    // Read from file source
    val df1 = spark.read.parquet("path/to/input/data")
    
    // Apply transformations with filters
    val filteredDf = df1
      .filter($"age" > 25)
      .filter($"status" === "active")
      .select("id", "name", "age", "salary")
    
    // Action that triggers the listener
    val result = filteredDf.collect()
    
    // Write operation
    filteredDf.write.mode("overwrite").parquet("path/to/output/data")
    
    spark.stop()
  }
}