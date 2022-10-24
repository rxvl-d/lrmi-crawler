package org.rxvl

import cats.effect.IO
import fs2.io.file
import fs2.text
import org.apache.any23.Any23
import org.apache.any23.extractor.{ExtractionContext, ExtractorGroup}
import org.apache.any23.extractor.html.*
import org.apache.any23.extractor.microdata.MicrodataExtractorFactory
import org.apache.any23.writer.{LoggingTripleHandler, NTriplesWriter, TripleHandler}
import org.apache.commons.io.output.ByteArrayOutputStream
import org.eclipse.rdf4j.model.{IRI, Resource, Value}
import org.joda.time.DateTime
import org.jsoup.Jsoup

import scala.collection.mutable
import scala.jdk.CollectionConverters.*

sealed trait WARCSegment

case class WARCInfo(description: String, timestamp: DateTime) extends WARCSegment
case class WARCResponse(uri: String, lrmi: LRMI) extends WARCSegment
case class WARCRequest(uri: String) extends WARCSegment
case object WARCSkip extends WARCSegment

case class LRMI(recorded: List[(String, String)]) {
  def isDefined: Boolean = recorded.nonEmpty
}

class RecordingHandler extends TripleHandler {
  var iri: IRI = null
  var triples: mutable.Buffer[(Resource, IRI, Value)] = scala.collection.mutable.Buffer[(Resource, IRI, Value)]()
  override def startDocument(documentIRI: IRI): Unit = {
    this.iri = documentIRI
  }

  override def openContext(context: ExtractionContext): Unit = ()

  override def receiveTriple(s: Resource, p: IRI, o: Value, g: IRI, context: ExtractionContext): Unit = {
    triples.append((s, p, o))
  }

  override def receiveNamespace(prefix: String, uri: String, context: ExtractionContext): Unit = ()

  override def closeContext(context: ExtractionContext): Unit = ()

  override def endDocument(documentIRI: IRI): Unit = ()

  override def setContentLength(contentLength: Long): Unit = ()

  override def close(): Unit = ()
}

object WARCParser {

  private val relevantItemProps = List(
    "alignmentType",
    "assesses",
    "educationalAlignment",
    "educationalFramework",
    "educationalLevel",
    "educationalRole",
    "educationalUse",
    "interactivityType",
    "isBasedOnUrl",
    "learningResourceType",
    "targetDescription",
    "targetName",
    "targetURL",
    "teaches",
    "timeRequired",
    "typicalAgeRange",
    "useRightsURL"
  )
  def extractLRMI(url: String, contentType: String, html: String): IO[LRMI] = IO {
    val runner = new Any23(new ExtractorGroup(List(
      new EmbeddedJSONLDExtractorFactory(),
      new MicrodataExtractorFactory(),
      new HCardExtractorFactory(),
      new HTMLMetaExtractorFactory(),
      new TurtleHTMLExtractorFactory()
    ).asJava))
    val recorder = new RecordingHandler()
    runner.extract(html, url, contentType, "utf-8", recorder)
    val triples = recorder.triples.filter({
      case (s, p, v) => relevantItemProps.exists(p.toString.contains)})
    LRMI(triples.toList.map({case (s, p, v) => (p.stringValue(), v.stringValue())}))
  }.handleErrorWith({
    error => IO(System.err.println(s"ERROR [$url] [$error]")).map(_ => LRMI(Nil))
  })

  def toSegment(segmentChunk: Vector[String]): IO[WARCSegment] = {
    val headerSeparatorIndex = segmentChunk.indexOf("")
    val (header, content) = segmentChunk.splitAt(headerSeparatorIndex)
    val segmentType = header.find(_.startsWith("WARC-Type")).get.split(": ").last
    if (segmentType == "warcinfo") { IO {
        val description = content.find(_.startsWith("description")).get
        val t = DateTime.parse(header.find(_.startsWith("WARC-Date")).get.split(": ").last)
        WARCInfo(description, t)
      }.handleError(_ => WARCSkip)
    } else if (segmentType == "response") {
      val responseHeaderSeparatorIndex = content.dropWhile(_ == "").indexOf("")
      val (responseHeader, responseContents) = content.splitAt(responseHeaderSeparatorIndex)
      val contentTypeIsHTML = responseHeader.find(_.startsWith("Content-Type")).exists(_.contains("text/html"))
      if (contentTypeIsHTML) {
        val responseContent = responseContents.mkString("\n")
        val url = header.find(_.startsWith("WARC-Target-URI")).get.split(": ")(1)
        val lrmi = extractLRMI(url, "text/html", responseContent)
        lrmi.map(WARCResponse(url, _))
      } else IO.pure(WARCSkip)
    }
    else IO.pure(WARCSkip)
  }

  def parse(fileUrl: String, lines: fs2.Stream[IO, String]): IO[List[WARCSegment]] = {
    lines
//      .evalMap(printItemProp(fileUrl))
      .groupAdjacentBy(_.startsWith("WARC/1.0"))
      .filter(x => !x._1)
      .map(_._2.toVector)
      .map(toSegment)
      .filter({case WARCResponse(_, lrmi) => lrmi.isDefined; case _ => false})
      .compile
      .toList
  }
}