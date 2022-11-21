package org.rxvl

import cats.effect.IO
import org.eclipse.rdf4j.model.Statement
import org.eclipse.rdf4j.query.QueryResults

import scala.collection.mutable.ListBuffer
//import cats.implicits.*
//import cats.data.Validated.*
import cats.data.ValidatedNec
import org.eclipse.rdf4j.rio.{RDFFormat, Rio}

import java.io.InputStream

object WDCParser {

//  private def toQuad(line: String, lineNumber: Long): ValidatedNec[String, (String, String, String, String)] = {
//    val segments = line.split("""\s+""")
//    val s = segments.lift(0).map(_.validNec).getOrElse(s"$line:$lineNumber s missing".invalidNec)
//    val v = segments.lift(0).map(_.validNec).getOrElse(s"$line:$lineNumber v missing".invalidNec)
//    val o = segments.lift(0).map(_.validNec).getOrElse(s"$line:$lineNumber o missing".invalidNec)
//    val url = segments.lift(0).map(_.validNec).getOrElse(s"$line:$lineNumber url missing".invalidNec)
//    (s, v, o, url).tupled
//  }
//
//  def logErrors[T](in: ValidatedNec[String, T]): IO[Option[T]] = in.fold(
//    es => IO(es.map(System.err.println)).map(_ => None),
//    t => IO.pure(Some(t)))
//
//  def extractWDC(lines: fs2.Stream[IO, String]): IO[List[(String, String, String, String)]] = {
//    lines
//      .filter(_.stripLineEnd.nonEmpty)
//      .zipWithIndex
//      .map(Function.tupled(toQuad))
//      .evalMap(logErrors)
//      .collect { case Some(t) => t }
//      .filter(t => WARCParser.isRelevantTriple(t._2))
//      .compile
//      .toList
//  }

  def extractWDCStream(is: InputStream): IO[List[(String, String, String, String)]] = IO {
    val res = QueryResults.parseGraphBackground(
      is,
      "http://data.dws.informatik.uni-mannheim.de/structureddata/",
      RDFFormat.NQUADS)
    val lrmiStatements = ListBuffer[Statement]()
    while(res.hasNext) {
      val statement = res.next()
      if (lrmiPropURLs.contains(statement.getPredicate.stringValue())) {
        lrmiStatements.append(statement)
      }
    }
    lrmiStatements.toList.map(s => (
      s.getSubject.stringValue(), 
      s.getPredicate.stringValue(),
      s.getObject.stringValue(), 
      s.getContext.stringValue()))
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
      Set(RDFURI))
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
