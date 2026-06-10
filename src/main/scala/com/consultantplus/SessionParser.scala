package com.consultantplus

object SessionParser {

  private val TargetDocId = "ACC_45616"

  case class QsDocOpen(date: String, docId: String)

  case class SessionResult(
    sessionDate:    String,
    cardSearchHits: Int,
    malformedLines: Int,
    qsDocOpens:     Seq[QsDocOpen]
  )

  private def extractDate(datetime: String): String =
    datetime.split("_").headOption.getOrElse(datetime)

  def parse(content: String): SessionResult = {
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

    SessionResult(sessionDate, cardSearchHits, malformedLines, qsDocOpens.toSeq)
  }
}
