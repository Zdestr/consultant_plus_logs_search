package com.consultantplus.jobs

import io.delta.tables.DeltaTable
import io.prometheus.client.{CollectorRegistry, Counter => PCounter, Gauge => PGauge}
import io.prometheus.client.exporter.PushGateway
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object PushMetrics {

  def run(spark: SparkSession, deltaPath: String, jobName: String): Unit = {
    val pgwUrl = sys.env.getOrElse("PUSHGATEWAY_URL", "")
    if (pgwUrl.isEmpty) {
      println("[PushMetrics] PUSHGATEWAY_URL not set, skipping.")
      return
    }

    val rawSessionsPath = s"$deltaPath/raw/sessions"
    val processedPath   = s"$deltaPath/processed/daily_opens"

    val row = spark.read.format("delta").load(rawSessionsPath)
      .agg(
        sum("card_search_hits").alias("total_hits"),
        sum("malformed_lines").alias("total_malformed"),
        count("file_path").alias("total_files")
      ).first()

    val totalHits      = if (row.isNullAt(0)) 0L else row.getLong(0)
    val totalMalformed = if (row.isNullAt(1)) 0L else row.getLong(1)
    val totalFiles     = row.getLong(2)

    val totalDocOpens = spark.read.format("delta").load(processedPath)
      .agg(sum("opens")).first().getLong(0)

    val registry = new CollectorRegistry()

    PGauge.build().name("cp_pipeline_files_total")
      .help("Total session files processed").register(registry)
      .set(totalFiles.toDouble)

    PGauge.build().name("cp_pipeline_card_search_hits")
      .help("Total card search hits for target document").register(registry)
      .set(totalHits.toDouble)

    PGauge.build().name("cp_pipeline_doc_opens_total")
      .help("Total QS document opens").register(registry)
      .set(totalDocOpens.toDouble)

    PGauge.build().name("cp_pipeline_malformed_lines")
      .help("Total malformed lines skipped").register(registry)
      .set(totalMalformed.toDouble)

    new PushGateway(pgwUrl).pushAdd(registry, jobName)
    println(s"[PushMetrics] Pushed to $pgwUrl (job=$jobName)")
  }
}
