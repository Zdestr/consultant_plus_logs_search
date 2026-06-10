package com.consultantplus

import com.consultantplus.jobs._
import org.apache.spark.sql.SparkSession

object Main {

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      System.err.println("Usage: Main <command> <logs-path> <delta-path> [target-date]")
      System.err.println("Commands: ingest | process | aggregate | metrics | all")
      sys.exit(1)
    }

    val command   = args(0)
    val logsPath  = args(1)
    val deltaPath = args(2)
    val targetDate = if (args.length >= 4) Some(args(3)) else None

    val spark = SparkSession.builder()
      .appName(s"ConsultantPlus-$command")
      .master(sys.env.getOrElse("SPARK_MASTER", "local[*]"))
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try {
      command match {
        case "ingest"    => IngestRaw.run(spark, logsPath, deltaPath)
        case "process"   => ProcessSessions.run(spark, deltaPath, targetDate)
        case "aggregate" => AggregateMetrics.run(spark, deltaPath)
        case "metrics"   => PushMetrics.run(spark, deltaPath, "cp-pipeline")
        case "all" =>
          IngestRaw.run(spark, logsPath, deltaPath)
          ProcessSessions.run(spark, deltaPath, targetDate)
          AggregateMetrics.run(spark, deltaPath)
          PushMetrics.run(spark, deltaPath, "cp-pipeline")
        case other =>
          System.err.println(s"Unknown command: $other")
          sys.exit(1)
      }
    } finally {
      spark.stop()
    }
  }
}
