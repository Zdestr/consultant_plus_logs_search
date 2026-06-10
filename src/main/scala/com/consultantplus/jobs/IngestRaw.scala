package com.consultantplus.jobs

import com.consultantplus.SessionParser
import io.delta.tables.DeltaTable
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object IngestRaw {

  val RawSessionSchema: StructType = StructType(Seq(
    StructField("file_path",        StringType,  nullable = false),
    StructField("date",             StringType,  nullable = false),
    StructField("card_search_hits", IntegerType, nullable = false),
    StructField("malformed_lines",  IntegerType, nullable = false)
  ))

  val RawDocOpenSchema: StructType = StructType(Seq(
    StructField("file_path", StringType, nullable = false),
    StructField("date",      StringType, nullable = false),
    StructField("doc_id",    StringType, nullable = false)
  ))

  def run(spark: SparkSession, logsPath: String, deltaPath: String): Unit = {
    val sc = spark.sparkContext
    import spark.implicits._

    val rawSessionsPath = s"$deltaPath/raw/sessions"
    val rawDocOpensPath = s"$deltaPath/raw/doc_opens"

    initDeltaTable(spark, rawSessionsPath, RawSessionSchema,  partitionBy = None)
    initDeltaTable(spark, rawDocOpensPath,  RawDocOpenSchema, partitionBy = Some("date"))

    val processedPaths: Set[String] =
      spark.read.format("delta").load(rawSessionsPath)
        .select("file_path").as[String].collect().toSet

    val newFiles = sc.binaryFiles(logsPath)
      .filter { case (path, _) => !processedPaths.contains(path) }

    val newCount = newFiles.count()
    if (newCount == 0) {
      println("[IngestRaw] No new files.")
      return
    }
    println(s"[IngestRaw] Processing $newCount new file(s).")

    val malformedAcc = sc.longAccumulator("malformed_lines")

    val parsedRDD = newFiles.flatMap { case (path, portable) =>
      try {
        val content = new String(portable.toArray(), "UTF-8")
        val result  = SessionParser.parse(content)
        malformedAcc.add(result.malformedLines)
        Seq((path, result))
      } catch {
        case e: Exception =>
          System.err.println(s"[WARN] Cannot parse $path: ${e.getMessage}")
          Seq.empty
      }
    }

    parsedRDD.cache()

    val sessionsDf = spark.createDataFrame(
      parsedRDD.map { case (path, r) => Row(path, r.sessionDate, r.cardSearchHits, r.malformedLines) },
      RawSessionSchema
    )

    val docOpensDf = spark.createDataFrame(
      parsedRDD.flatMap { case (path, r) => r.qsDocOpens.map(o => Row(path, o.date, o.docId)) },
      RawDocOpenSchema
    )

    DeltaTable.forPath(spark, rawSessionsPath).as("t")
      .merge(sessionsDf.as("s"), "t.file_path = s.file_path")
      .whenNotMatchedInsertAll()
      .execute()

    DeltaTable.forPath(spark, rawDocOpensPath).as("t")
      .merge(docOpensDf.as("s"),
        "t.file_path = s.file_path AND t.doc_id = s.doc_id")
      .whenNotMatchedInsertAll()
      .execute()

    parsedRDD.unpersist()

    println(s"[IngestRaw] Done. Malformed lines: ${malformedAcc.value}")

    DeltaTable.forPath(spark, rawSessionsPath)
      .history(5).select("version", "timestamp", "operation")
      .show(truncate = false)
  }

  private def initDeltaTable(
    spark: SparkSession,
    path: String,
    schema: StructType,
    partitionBy: Option[String]
  ): Unit = {
    if (!DeltaTable.isDeltaTable(spark, path)) {
      val w = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
        .write.format("delta")
      partitionBy.fold(w)(w.partitionBy(_)).save(path)
    }
  }
}
