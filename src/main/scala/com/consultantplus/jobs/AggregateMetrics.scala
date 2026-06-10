package com.consultantplus.jobs

import io.delta.tables.DeltaTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object AggregateMetrics {

  private val DocMetadata = Seq(
    ("ACC", "Арбитражная практика"),
    ("LAW", "Законодательство"),
    ("CMB", "Комментарии"),
    ("CJI", "Судебная практика"),
    ("PBI", "Правовой вестник")
  )

  def run(
    spark: SparkSession,
    deltaPath: String,
    targetDocId: String = "ACC_45616"
  ): Unit = {
    import spark.implicits._

    val rawSessionsPath = s"$deltaPath/raw/sessions"
    val processedPath   = s"$deltaPath/processed/daily_opens"
    val analyticsPath   = s"$deltaPath/analytics/summary"

    val totalCardHits =
      spark.read.format("delta").load(rawSessionsPath)
        .select("card_search_hits")
        .agg(sum("card_search_hits"))
        .first().getLong(0)

    println(s"\n=== Метрика 1: card-search hits для $targetDocId: $totalCardHits ===\n")

    val opensDf = spark.read.format("delta").load(processedPath)
      .select("date", "doc_id", "opens")

    val metaDf = spark.createDataset(DocMetadata).toDF("prefix", "category")

    val enriched = opensDf
      .withColumn("prefix", regexp_extract(col("doc_id"), "^([A-Z]+)_", 1))
      .join(metaDf, Seq("prefix"), "left")
      .drop("prefix")
      .orderBy(col("date").asc, col("doc_id").asc)

    println("=== Метрика 2: открытия документов через быстрый поиск ===")
    enriched.show(50, truncate = false)

    if (!DeltaTable.isDeltaTable(spark, analyticsPath)) {
      enriched.write.format("delta").partitionBy("date").save(analyticsPath)
    } else {
      DeltaTable.forPath(spark, analyticsPath).as("t")
        .merge(enriched.as("u"),
          "t.date = u.date AND t.doc_id = u.doc_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    }

    println(s"[AggregateMetrics] Analytics saved to $analyticsPath")
  }
}
