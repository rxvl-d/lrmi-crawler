package org.rxvl

import cats.effect.unsafe.implicits.global

import java.io.{ByteArrayInputStream, StringBufferInputStream, StringReader}
import java.nio.charset.StandardCharsets


class WDCParserTest extends munit.FunSuite {
  test("WDCParser") {
    val toExtract = List(
//      """_:node1fm7c5gkjx42317463 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://schema.org/WebPage> <http://1956.konyvtar-siofok.hu/index.php/sarkozi-matyas-torkig-bizanccal/>   .""",
      """_:node1fm7c5gkjx42317463 <http://schema.org/breadcrumb> "Kezdőlap » Forradalom a könyvekben » Sárközi Mátyás: Torkig Bizánccal"@hu <http://1956.konyvtar-siofok.hu/index.php/sarkozi-matyas-torkig-bizanccal/>   .""".stripMargin
    )
    val extracted = WDCParser.extractWDC(
      fs2.Stream.evalSeq(cats.effect.IO(toExtract))
    ).unsafeRunSync()
    assertEquals(extracted, List(
      (
        "http://1000lifehacks.com/#organization",
        "http://schema.org/educationalLevel",
        "http://1000lifehacks.com/#logo",
        "http://1000lifehacks.com/how-to-remember-stuff-in-the-morning/"
      )
    )
    )
  }
}
