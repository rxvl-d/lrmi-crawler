package org.rxvl

import cats.effect.IO
import fs2.io.file
import fs2.text
import org.joda.time.DateTime
import org.jsoup.Jsoup
import scala.jdk.CollectionConverters.*

sealed trait WARCSegment

case class WARCInfo(description: String, timestamp: DateTime) extends WARCSegment
case class WARCResponse(uri: String, lrmi: LRMI) extends WARCSegment
case object WARCSkip extends WARCSegment

case class LRMI(learningResourceType: Option[String],
                isBasedOnUrl: Option[String],
                audience: Option[String],
                educationalAlignment: List[String]) {
  def isDefined = learningResourceType.isDefined || isBasedOnUrl.isDefined || audience.isDefined || educationalAlignment.nonEmpty
}

object WARCParser {

  def extractLRMI(html: String): LRMI = {
    val parsed = Jsoup.parse(html)
    val learningResourceType = Option(parsed.select("[itemprop=learningResourceType]").first()).map(_.text())
    val isBasedOnUrl = Option(parsed.select("[itemprop=isBasedOnUrl]").first()).map(_.attr("href"))
    val audience = Option(parsed.select("[itemprop=audience] > [itemprop=educationalRole]").first()).map(_.text())
    val educationalAlignemnt = parsed.select("[itemprop=educationalAlignment] > [itemprop=targetName]"
    ).eachText().asScala.toList
    LRMI(learningResourceType, isBasedOnUrl, audience, educationalAlignemnt)
  }

  def toSegment(segmentChunk: Vector[String]): WARCSegment = {
    val headerSeparatorIndex = segmentChunk.indexOf("")
    val (header, content) = segmentChunk.splitAt(headerSeparatorIndex)
    val segmentType = header.find(_.startsWith("WARC-Type")).get.split(": ").last
    val out = if (segmentType == "warcinfo") {
      val description = content.find(_.startsWith("description")).get
      val t = DateTime.parse(header.find(_.startsWith("WARC-Date")).get.split(": ").last)
      WARCInfo(description, t)
    } else if (segmentType == "response") {
      val responseHeaderSeparatorIndex = content.dropWhile(_ == "").indexOf("")
      val (responseHeader, responseContents) = content.splitAt(responseHeaderSeparatorIndex)
      val contentTypeIsHTML = responseHeader.find(_.startsWith("Content-Type")).exists(_.contains("text/html"))
      if (contentTypeIsHTML) {
        val responseContent = responseContents.mkString("\n")
        val url = header.find(_.startsWith("WARC-Target-URI")).get.split(": ")(1)
        val lrmi = extractLRMI(responseContent)
        WARCResponse(url, lrmi)
      } else WARCSkip
    }
    else WARCSkip
    out
  }

  def printItemProp(line: String): IO[String] = IO {
    if (line.contains("itemprop")) {println(line)}
    line
  }

  def parse(lines: fs2.Stream[IO, String]): IO[List[WARCSegment]] = {
    lines
      .evalMap(printItemProp)
      .groupAdjacentBy(_.startsWith("WARC/1.0"))
      .filter(x => !x._1)
      .map(_._2.toVector)
      .map(toSegment)
      .filter({case WARCResponse(_, lrmi) => lrmi.isDefined; case _ => false})
      .compile
      .toList
  }
}