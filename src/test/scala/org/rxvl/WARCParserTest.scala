package org.rxvl

import cats.effect.IO
import org.eclipse.rdf4j.model.impl.{SimpleBNode, SimpleIRI, SimpleLiteral}
import org.eclipse.rdf4j.model.util.Literals

import java.io.BufferedInputStream

//class WARCParserTest extends munit.FunSuite {
//  test("Test WARCParser;") {
//    import cats.effect.unsafe.implicits.global
//    val segments = WARCParser.parse(
//      resourceToLines("test.warc")).unsafeRunSync()
//    assertEquals(segments, List(WARCResponse(
//      uri = "http://11sqft.com/?xyz/",
//      lrmi = LRMI(
//        recorded = List(
//          (
//            "http://schema.org/learningResourceType",
//            "news"
//          )
//        )
//      )
//    )))
//  }
//
//  def resourceToLines(filename: String): fs2.Stream[IO, String] = {
//    val bis = IO({
//      val is = getClass.getClassLoader.getResourceAsStream(filename)
//      new BufferedInputStream(is)
//    })
//    fs2.io.readInputStream[IO](bis, 4096, closeAfterUse = true)
//      .through(fs2.text.utf8.decode)
//      .through(fs2.text.lines)
//
//  }
//}
