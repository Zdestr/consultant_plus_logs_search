package com.consultantplus

import org.apache.spark.{SparkConf, SparkContext}

object LogAnalysis {

  private val TargetDocId = "ACC_45616"

  private def extractDate(datetime: String): String =
    datetime.split("_").headOption.getOrElse(datetime)

  private case class QsDocOpen(date: String, docId: String)

  private def parseSession(content: String): (Int, Seq[QsDocOpen]) = {
    val lines = content.linesIterator.map(_.trim).filter(_.nonEmpty).toArray

    var cardSearchHits  = 0
    val qsDocOpens      = scala.collection.mutable.ArrayBuffer[QsDocOpen]()
    val searchIndex     = scala.collection.mutable.Map.empty[String, (String, String)]

    var sessionDate    = ""
    var lastType       = ""
    var lastSearchDate = ""
    var awaitingResult = false

    for (line <- lines) {
      val tokens = line.split("\\s+")
      val tag    = tokens(0)

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
            case _ => ("", "", "")
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

    (cardSearchHits, qsDocOpens.toSeq)
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: LogAnalysis <path-to-docs-directory>")
      sys.exit(1)
    }
    val docsPath = args(0)

    val conf = new SparkConf()
      .setAppName("ConsultantPlusLogAnalysis")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val sessions  = sc.wholeTextFiles(docsPath)
    val parsedRDD = sessions.map { case (_, content) => parseSession(content) }.cache()

    val cardSearchCount: Long = parsedRDD.map(_._1.toLong).sum().toLong

    val docOpensByDay = parsedRDD
      .flatMap(_._2)
      .map(e => ((e.date, e.docId), 1L))
      .reduceByKey(_ + _)
      .map { case ((date, docId), count) => (date, docId, count) }
      .collect()
      .sortBy { case (date, docId, _) =>
        val p = date.split("\\.")
        val chronoKey = if (p.length == 3) s"${p(2)}.${p(1)}.${p(0)}" else date
        (chronoKey, docId)
      }

    sc.stop()

    println()
    println("=" * 60)
    println(s"Metric 1 — Card searches that returned $TargetDocId")
    println("=" * 60)
    println(s"  Count: $cardSearchCount")

    println()
    println("=" * 60)
    println("Metric 2 — Document openings per day (via quick search)")
    println("=" * 60)
    println(f"  ${"Date"}%-15s ${"Document ID"}%-20s ${"Opens"}")
    println("  " + "-" * 45)
    docOpensByDay.foreach { case (date, docId, count) =>
      println(f"  $date%-15s $docId%-20s $count")
    }
    println()
  }
}
