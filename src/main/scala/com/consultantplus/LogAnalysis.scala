package com.consultantplus

import io.delta.tables.DeltaTable
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object LogAnalysis {

  private val TargetDocId = "ACC_45616"

  private def extractDate(datetime: String): String =
    datetime.split("_").headOption.getOrElse(datetime)

  private case class QsDocOpen(date: String, docId: String)

  private def parseSession(content: String): (Int, Int, Seq[QsDocOpen]) = {
    val lines = content.linesIterator.map(_.trim).filter(_.nonEmpty).toArray

    var cardSearchHits  = 0
    var malformedLines  = 0
    val qsDocOpens      = scala.collection.mutable.ArrayBuffer[QsDocOpen]()
    val searchIndex     = scala.collection.mutable.Map.empty[String, (String, String)]

    var sessionDate    = ""
    var lastType       = ""
    var lastSearchDate = ""
    var awaitingResult = false

    for (line <- lines) {
      val tokens = line.split("\\s+")
      if (tokens.isEmpty) {
        System.err.println(s"[WARN] Skipping unexpectedly empty token array for line: '$line'")
        malformedLines += 1
      } else {
        val tag = tokens(0)

        tag match {
          case "SESSION_START" if tokens.length >= 2 =>
            sessionDate    = extractDate(tokens(1))
            awaitingResult = false

          case "QS" =>
            lastType       = "QS"
            lastSearchDate = if (tokens.length >= 2) extractDate(tokens(1)) else sessionDate
            awaitingResult = true

          case "CARD_SEARCH_START" =>
            lastType       = "CARD"
            awaitingResult = false

          case "CARD_SEARCH_END" =>
            awaitingResult = true

          case "DOC_OPEN" =>
            val (date, searchId, docId) = tokens.length match {
              case 4 => (extractDate(tokens(1)), tokens(2), tokens(3))
              case 3 => (sessionDate,            tokens(1), tokens(2))
              case _ =>
                System.err.println(s"[WARN] Malformed DOC_OPEN line (${tokens.length} tokens): '$line'")
                malformedLines += 1
                ("", "", "")
            }
            if (searchId.nonEmpty && docId.nonEmpty) {
              searchIndex.get(searchId).foreach {
                case ("QS", _) =>
                  qsDocOpens += QsDocOpen(if (date.nonEmpty) date else sessionDate, docId)
                case _ =>
              }
            }

          case "SESSION_END" =>
            awaitingResult = false

          case _ =>
            if (awaitingResult && !line.startsWith("$") && tokens.nonEmpty) {
              val searchId = tokens(0)
              val docs     = tokens.drop(1).toSet

              if (lastType == "QS") {
                searchIndex(searchId) = ("QS", lastSearchDate)
              } else if (lastType == "CARD") {
                if (docs.contains(TargetDocId)) cardSearchHits += 1
                searchIndex(searchId) = ("CARD", "")
              }

              awaitingResult = false
            }
        }
      }
    }

    (cardSearchHits, malformedLines, qsDocOpens.toSeq)
  }

  private val SessionSchema = StructType(Seq(
    StructField("file_path",        StringType,  nullable = false),
    StructField("card_search_hits", IntegerType, nullable = false),
    StructField("malformed_lines",  IntegerType, nullable = false)
  ))

  private val DocOpenSchema = StructType(Seq(
    StructField("file_path", StringType, nullable = false),
    StructField("date",      StringType, nullable = false),
    StructField("doc_id",    StringType, nullable = false)
  ))

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      System.err.println("Usage: LogAnalysis <path-to-docs-directory> <path-to-delta-lake>")
      sys.exit(1)
    }
    val docsPath  = args(0)
    val deltaPath = args(1)

    val spark = SparkSession.builder()
      .appName("ConsultantPlusLogAnalysis")
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val sc = spark.sparkContext

    val sessionsPath = s"$deltaPath/sessions"
    val docOpensPath = s"$deltaPath/doc_opens"

    try {
      if (!DeltaTable.isDeltaTable(spark, sessionsPath)) {
        spark.createDataFrame(sc.emptyRDD[Row], SessionSchema)
          .write.format("delta").save(sessionsPath)
      }
      if (!DeltaTable.isDeltaTable(spark, docOpensPath)) {
        spark.createDataFrame(sc.emptyRDD[Row], DocOpenSchema)
          .write.format("delta").partitionBy("date").save(docOpensPath)
      }

      import spark.implicits._
      val processedPaths = spark.read.format("delta").load(sessionsPath)
        .select("file_path").as[String].collect().toSet

      val newFiles = sc.wholeTextFiles(docsPath)
        .filter { case (path, _) => !processedPaths.contains(path) }

      val newFileCount = newFiles.count()
      if (newFileCount > 0) {
        println(s"Processing $newFileCount new file(s)...")

        val parsedRDD = newFiles.map { case (path, content) =>
          try {
            val (hits, malformed, opens) = parseSession(content)
            (path, hits, malformed, opens)
          } catch {
            case e: Exception =>
              System.err.println(s"[ERROR] Failed to parse $path: ${e.getMessage}")
              (path, 0, 0, Seq.empty[QsDocOpen])
          }
        }.cache()

        spark.createDataFrame(
          parsedRDD.map { case (path, hits, malformed, _) => Row(path, hits, malformed) },
          SessionSchema
        ).write.format("delta").mode("append").save(sessionsPath)

        spark.createDataFrame(
          parsedRDD.flatMap { case (path, _, _, opens) =>
            opens.map(o => Row(path, o.date, o.docId))
          },
          DocOpenSchema
        ).write.format("delta").mode("append").save(docOpensPath)

        parsedRDD.unpersist()
      } else {
        println("No new files — computing metrics from cached Delta data.")
      }

      val sessionsDf = spark.read.format("delta").load(sessionsPath)
      val docOpensDf = spark.read.format("delta").load(docOpensPath)

      val totalsRow       = sessionsDf.agg(sum("card_search_hits"), sum("malformed_lines")).first()
      val cardSearchCount    = if (totalsRow.isNullAt(0)) 0L else totalsRow.getLong(0)
      val malformedLineCount = if (totalsRow.isNullAt(1)) 0L else totalsRow.getLong(1)

      val docOpensByDay = docOpensDf
        .groupBy("date", "doc_id")
        .agg(count("*").alias("opens"))
        .collect()
        .sortBy { row =>
          val date = row.getString(0)
          val p    = date.split("\\.")
          val key  = if (p.length == 3) s"${p(2)}.${p(1)}.${p(0)}" else date
          (key, row.getString(1))
        }

      println()
      println("=" * 60)
      println(s"Metric 1 — Card searches that returned $TargetDocId")
      println("=" * 60)
      println(s"  Count: $cardSearchCount")

      println()
      println("=" * 60)
      println("Metric 2 — Document openings per day (via quick search)")
      println("=" * 60)

      if (docOpensByDay.isEmpty)
        System.err.println(s"[WARN] No QS doc opens found — check input format or TargetDocId '$TargetDocId'")

      println(f"  ${"Date"}%-15s ${"Document ID"}%-20s ${"Opens"}")
      println("  " + "-" * 45)
      docOpensByDay.foreach { row =>
        println(f"  ${row.getString(0)}%-15s ${row.getString(1)}%-20s ${row.getLong(2)}")
      }

      if (malformedLineCount > 0) {
        println()
        println("=" * 60)
        println("Metric 3 — Malformed (skipped) lines")
        println("=" * 60)
        println(s"  Count: $malformedLineCount")
        println()
      }
    } finally {
      spark.stop()
    }
  }
}
