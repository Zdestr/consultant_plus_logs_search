package com.consultantplus.jobs

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object ProcessSessions {

  private val ProcessedDocOpenSchema = StructType(Seq(
    StructField("date",   StringType, nullable = false),
    StructField("doc_id", StringType, nullable = false),
    StructField("opens",  LongType,   nullable = false)
  ))

  def run(
    spark: SparkSession,
    deltaPath: String,
    targetDate: Option[String] = None
  ): Unit = {
    val rawDocOpensPath    = s"$deltaPath/raw/doc_opens"
    val processedOpensPath = s"$deltaPath/processed/daily_opens"

    if (!DeltaTable.isDeltaTable(spark, processedOpensPath)) {
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row], ProcessedDocOpenSchema)
        .write.format("delta").partitionBy("date").save(processedOpensPath)
    }

    var rawDf = spark.read.format("delta").load(rawDocOpensPath)
      .select("date", "doc_id")

    targetDate.foreach { d => rawDf = rawDf.filter(col("date") === d) }

    val aggregated = rawDf
      .groupBy("date", "doc_id")
      .agg(count("*").alias("opens"))

    DeltaTable.forPath(spark, processedOpensPath).as("t")
      .merge(aggregated.as("u"),
        "t.date = u.date AND t.doc_id = u.doc_id")
      .whenMatched().update(Map("opens" -> col("u.opens")))
      .whenNotMatched().insertAll()
      .execute()

    println(s"[ProcessSessions] Done for date=${targetDate.getOrElse("all")}.")
  }
}
