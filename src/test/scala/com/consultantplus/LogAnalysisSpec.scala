package com.consultantplus

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LogAnalysisSpec extends AnyFlatSpec with Matchers {

  private def parse(content: String) = LogAnalysis.parseSession(content)

  "parseSession" should "return zeros and empty opens for an empty session" in {
    val (hits, malformed, opens) = parse("")
    hits      shouldBe 0
    malformed shouldBe 0
    opens     shouldBe empty
  }

  it should "count a QS doc open when DOC_OPEN references the QS search id" in {
    val content =
      """SESSION_START 01.07.2020_10:00:00
        |QS 01.07.2020_10:01:00 {query}
        |999 ACC_45616 LAW_123
        |DOC_OPEN 01.07.2020_10:02:00 999 ACC_45616
        |SESSION_END 01.07.2020_10:03:00""".stripMargin

    val (hits, malformed, opens) = parse(content)
    hits      shouldBe 0
    malformed shouldBe 0
    opens     should have size 1
    opens.head shouldBe LogAnalysis.QsDocOpen("01.07.2020", "ACC_45616")
  }

  it should "count multiple QS doc opens across different searches" in {
    val content =
      """SESSION_START 25.12.2020_09:00:00
        |QS 25.12.2020_09:01:00 {first}
        |111 DOC_A DOC_B
        |DOC_OPEN 25.12.2020_09:02:00 111 DOC_A
        |DOC_OPEN 25.12.2020_09:03:00 111 DOC_B
        |QS 25.12.2020_09:04:00 {second}
        |222 DOC_C
        |DOC_OPEN 25.12.2020_09:05:00 222 DOC_C
        |SESSION_END 25.12.2020_09:06:00""".stripMargin

    val (_, _, opens) = parse(content)
    opens should have size 3
    opens.map(_.docId) should contain allOf ("DOC_A", "DOC_B", "DOC_C")
  }

  it should "count a card search hit when ACC_45616 appears in results" in {
    val content =
      """SESSION_START 07.07.2020_10:00:00
        |CARD_SEARCH_START 07.07.2020_10:01:00
        |$0 some query
        |CARD_SEARCH_END
        |888 ACC_45616 LAW_999
        |SESSION_END 07.07.2020_10:02:00""".stripMargin

    val (hits, _, _) = parse(content)
    hits shouldBe 1
  }

  it should "not count a card search hit when ACC_45616 is absent from results" in {
    val content =
      """SESSION_START 07.07.2020_10:00:00
        |CARD_SEARCH_START 07.07.2020_10:01:00
        |$0 some query
        |CARD_SEARCH_END
        |888 LAW_111 LAW_222
        |SESSION_END 07.07.2020_10:02:00""".stripMargin

    val (hits, _, _) = parse(content)
    hits shouldBe 0
  }

  it should "not count a DOC_OPEN as QS open when it references a CARD search id" in {
    val content =
      """SESSION_START 07.07.2020_10:00:00
        |CARD_SEARCH_START 07.07.2020_10:01:00
        |$0 query
        |CARD_SEARCH_END
        |777 DOC_X
        |DOC_OPEN 07.07.2020_10:02:00 777 DOC_X
        |SESSION_END 07.07.2020_10:03:00""".stripMargin

    val (_, _, opens) = parse(content)
    opens shouldBe empty
  }

  it should "count malformed DOC_OPEN lines (wrong token count)" in {
    val content =
      """SESSION_START 01.07.2020_10:00:00
        |DOC_OPEN 01.07.2020_10:01:00
        |SESSION_END 01.07.2020_10:02:00""".stripMargin

    val (_, malformed, _) = parse(content)
    malformed shouldBe 1
  }

  it should "use session date when DOC_OPEN has only 3 tokens" in {
    val content =
      """SESSION_START 15.08.2020_08:00:00
        |QS 15.08.2020_08:01:00 {query}
        |555 DOC_Y
        |DOC_OPEN 555 DOC_Y
        |SESSION_END 15.08.2020_08:02:00""".stripMargin

    val (_, _, opens) = parse(content)
    opens should have size 1
    opens.head.date shouldBe "15.08.2020"
  }

  it should "handle a session with both card search hits and QS opens" in {
    val content =
      """SESSION_START 20.09.2020_10:00:00
        |CARD_SEARCH_START 20.09.2020_10:01:00
        |$0 query
        |CARD_SEARCH_END
        |100 ACC_45616
        |QS 20.09.2020_10:02:00 {another}
        |200 ACC_45616
        |DOC_OPEN 20.09.2020_10:03:00 200 ACC_45616
        |SESSION_END 20.09.2020_10:04:00""".stripMargin

    val (hits, malformed, opens) = parse(content)
    hits      shouldBe 1
    malformed shouldBe 0
    opens     should have size 1
  }
}
