package org.rxvl

import cats.effect.IO
import org.eclipse.rdf4j.model.Statement
import org.eclipse.rdf4j.query.QueryResults
import org.eclipse.rdf4j.rio.helpers.{BasicParserSettings, NTriplesParserSettings}
import org.joda.time.DateTime

import java.io.{FilterReader, BufferedInputStream, BufferedReader, InputStreamReader, OutputStreamWriter}
import scala.collection.mutable.ListBuffer
import cats.data.ValidatedNec
import org.eclipse.rdf4j.rio.{RDFFormat, Rio}

import java.io.InputStream
import concurrent.duration.DurationInt

object WDCParser {

  private def toQuad(line: String, lineNumber: Long): ValidatedNec[String, (String, String, String, String)] = {
    import cats.implicits._
    NQUADParser(line).toValidatedNec.bimap(
      _.map(_.msg + s". N: $lineNumber"), 
      s => (s.s, s.p, s.v, s.url))
  }

  def logErrors[T](in: ValidatedNec[String, T]): IO[Option[T]] = in.fold(
    es => IO.pure(None),
    //es => IO(es.map(System.err.println)).map(_ => None),
    t => IO.pure(Some(t)))

  def observe[T](s: fs2.Stream[IO, Option[T]]): fs2.Stream[IO, Option[T]] = {
    s.evalMapAccumulate((0, 0)) { case ((countNones, countSomes), i) =>
      IO {
        val counts = i match {
          case Some(_) => (countNones, countSomes + 1)
          case None => (countNones + 1, countSomes)
        }
        val total = counts._1 + counts._2
        if (total % 1000000 == 0) {
          System.err.println(s"[${DateTime.now()}] Incorrect ${counts._1 / total}. At $total.")
        }
        (counts, i)
      }
    }.map(_._2)
  }

  def extractWDC(lines: fs2.Stream[IO, String]): IO[List[(String, String, String, String)]] = {
    observe(lines
      .filter(_.stripLineEnd.nonEmpty)
      .zipWithIndex
      .evalMap({case (l, n) => IO(toQuad(l, n)).timeout(2.seconds)})
      .evalMap(logErrors))
      .collect { case Some(t) => t }
      .filter(t => WARCParser.isRelevantTriple(t._2))
      .compile
      .toList
  }



  val lrmiProperties = Set(
    LRMIProperty(
      "alignmentType",
      Set(
        "http://purl.org/dcx/lrmi-terms/alignmentType",
        "http://schema.org/alignmentType"),
      Set(AlignmentObject),
      Set(RDFString, SKOSConcept)),
    LRMIProperty(
      "assesses",
      Set(
        "http://purl.org/dcx/lrmi-terms/assesses",
        "http://schema.org/assesses"),
      Set(LearningResource, CreativeWork),
      Set(DefinedTerm, SKOSConcept)),
    LRMIProperty(
      "educationalAlignment",
      Set(
        "http://purl.org/dcx/lrmi-terms/educationalAlignment",
        "http://schema.org/educationalAlignment"),
      Set(LearningResource, CreativeWork),
      Set(AlignmentObject)),
    LRMIProperty(
      "educationalFramework",
      Set(
        "http://purl.org/dcx/lrmi-terms/educationalFramework",
        "http://schema.org/educationalFramework"),
      Set(AlignmentObject),
      Set(RDFString)),
    LRMIProperty(
      "educationalLevel",
      Set(
        "http://purl.org/dcx/lrmi-terms/educationalLevel",
        "http://schema.org/educationalLevel"),
      Set(LearningResource, CreativeWork),
      Set(DefinedTerm, SKOSConcept)),
    LRMIProperty(
      "educationalRole",
      Set(
        "http://purl.org/dcx/lrmi-terms/educationalRole",
        "http://schema.org/educationalRole"),
      Set(EducationalAudience),
      Set(RDFString, SKOSConcept)),
    LRMIProperty(
      "educationalUse",
      Set(
        "http://purl.org/dcx/lrmi-terms/educationalUse",
        "http://schema.org/educationalUse"),
      Set(LearningResource, CreativeWork),
      Set(RDFString, SKOSConcept)),
    LRMIProperty(
      "interactivityType",
      Set(
        "http://purl.org/dcx/lrmi-terms/interactivityType",
        "http://schema.org/interactivityType"),
      Set(LearningResource, CreativeWork),
      Set(RDFString, SKOSConcept)),
    LRMIProperty(
      "isBasedOnUrl",
      Set(
        "http://purl.org/dcx/lrmi-terms/isBasedOnUrl",
        "http://schema.org/isBasedOnUrl",
        "http://schema.org/isBasedOn"),
      Set(CreativeWork),
      Set(RDFResource)),
    LRMIProperty(
      "learningResourceType",
      Set(
        "http://purl.org/dcx/lrmi-terms/learningResourceType",
        "http://schema.org/learningResourceType"),
      Set(LearningResource, CreativeWork),
      Set(RDFString, SKOSConcept)),
    LRMIProperty(
      "targetDescription",
      Set(
        "http://purl.org/dcx/lrmi-terms/targetDescription",
        "http://schema.org/targetDescription"),
      Set(AlignmentObject),
      Set(RDFString)),
    LRMIProperty(
      "targetName",
      Set(
        "http://purl.org/dcx/lrmi-terms/targetName",
        "http://schema.org/targetName"),
      Set(AlignmentObject),
      Set(RDFString)),
    LRMIProperty(
      "targetURL",
      Set(
        "http://purl.org/dcx/lrmi-terms/targetURL",
        "http://schema.org/targetURL"),
      Set(AlignmentObject),
      Set(RDFURI)),
    LRMIProperty(
      "teaches",
      Set(
        "http://purl.org/dcx/lrmi-terms/teaches",
        "http://schema.org/teaches"),
      Set(LearningResource, CreativeWork),
      Set(DefinedTerm, SKOSConcept)),
    LRMIProperty(
      "timeRequired",
      Set(
        "http://purl.org/dcx/lrmi-terms/timeRequired",
        "http://schema.org/timeRequired"),
      Set(CreativeWork),
      Set(Duration)),
    LRMIProperty(
      "typicalAgeRange",
      Set(
        "http://purl.org/dcx/lrmi-terms/typicalAgeRange",
        "http://schema.org/typicalAgeRange"),
      Set(CreativeWork),
      Set(RDFString)),
    LRMIProperty(
      "useRightsURL",
      Set(
        "http://purl.org/dcx/lrmi-terms/useRightsURL",
        "http://schema.org/license"),
      Set(CreativeWork),
      Set(RDFURI)),
    LRMIProperty("courseCode",Set("http://schema.org/courseCode"), Set(AsYetUndefined),Set(AsYetUndefined)),
    LRMIProperty("coursePrerequisite",Set("http://schema.org/coursePrerequisite"), Set(AsYetUndefined),Set(AsYetUndefined)),
    LRMIProperty("courseSku",Set("http://schema.org/courseSku"), Set(AsYetUndefined),Set(AsYetUndefined)),
    LRMIProperty("educationalCredentialAwarded",Set("http://schema.org/educationalCredentialAwarded"), Set(AsYetUndefined),Set(AsYetUndefined)),
    LRMIProperty("educationalOutcome",Set("http://schema.org/educationalOutcome"), Set(AsYetUndefined),Set(AsYetUndefined)),
    LRMIProperty("hasCourseInstance",Set("http://schema.org/hasCourseInstance"), Set(AsYetUndefined),Set(AsYetUndefined)),
    LRMIProperty("subjectOfStudy",Set("http://schema.org/subjectOfStudy"), Set(AsYetUndefined),Set(AsYetUndefined)),
    LRMIProperty("alignmentType",Set("http://schema.org/alignmentType"), Set(AsYetUndefined),Set(AsYetUndefined)),
    LRMIProperty("educationalFramework",Set("http://schema.org/educationalFramework"), Set(AsYetUndefined),Set(AsYetUndefined)),
    LRMIProperty("educationalAlignment",Set("http://schema.org/educationalAlignment"), Set(AsYetUndefined),Set(AsYetUndefined)),
    LRMIProperty("learningResourceType",Set("http://schema.org/learningResourceType"), Set(AsYetUndefined),Set(AsYetUndefined)),
    LRMIProperty("interactivityType",Set("http://schema.org/interactivityType"), Set(AsYetUndefined),Set(AsYetUndefined)),
    LRMIProperty("educationalUse",Set("http://schema.org/educationalUse"), Set(AsYetUndefined),Set(AsYetUndefined)),
    LRMIProperty("educationalLevel",Set("http://schema.org/educationalLevel"), Set(AsYetUndefined),Set(AsYetUndefined)),  
  )

  val lrmiPropURLs = lrmiProperties.flatMap(_.url)

}

case class LRMIProperty(shortName: String,
                        url: Set[String],
                        domain: Set[RDFTypes],
                        range: Set[RDFTypes])

sealed trait RDFTypes {
  def urls: Set[String]
}
case object AlignmentObject extends RDFTypes {
  val urls = Set(
    "http://purl.org/dcx/lrmi-terms/AlignmentObject",
    "http://schema.org/AlignmentObject"
  )
}
case object LearningResource extends RDFTypes {
  val urls = Set(
    "http://purl.org/dcx/lrmi-terms/LearningResource",
    "http://schema.org/LearningResource"
  )
}
case object EducationalAudience extends RDFTypes {
  val urls = Set(
    "http://purl.org/dcx/lrmi-terms/EducationalAudience",
    "http://schema.org/EducationalAudience "
  )
}
case object CreativeWork extends RDFTypes {
  val urls = Set(
    "http://schema.org/CreativeWork"
  )
}
case object Course extends RDFTypes {
  val urls = Set(
    "http://schema.org/Course"
  )
}
case object AsYetUndefined extends RDFTypes {
  val urls = Set(
    "sorry too lazy"
  )
}
case object RDFString extends RDFTypes {
  val urls = Set("http://www.w3.org/2001/XMLSchema#string")
}

case object RDFURI extends RDFTypes {
  val urls = Set("http://www.w3.org/2001/XMLSchema#anyURI")
}
case object RDFResource extends RDFTypes {
  val urls = Set("http://www.w3.org/2000/01/rdf-schema#Resource")
}

case object SKOSConcept extends RDFTypes {
  val urls = Set("http://www.w3.org/2004/02/skos/core#Concept")
}

case object DefinedTerm extends RDFTypes {
  val urls = Set("http://schema.org/DefinedTerm")
}

case object Duration extends RDFTypes {
  val urls = Set("http://schema.org/Duration")
}
