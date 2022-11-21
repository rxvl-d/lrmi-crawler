package org.rxvl

import cats.effect.unsafe.implicits.global

import java.io.{ByteArrayInputStream, StringBufferInputStream, StringReader}
import java.nio.charset.StandardCharsets


class WDCParserTest extends munit.FunSuite {
  test("WDCParser") {
    val toExtract =
      """_:node1fm7c5gkjx46202426 <http://schema.org/pricecurrency> "INR"@en_US.UTF-8 <https://www.lihaaj.com/abayas/denim-abaya>   .""".stripMargin
    val extracted = WDCParser.extractWDCStream(
      new ByteArrayInputStream(toExtract.getBytes(StandardCharsets.UTF_8))
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
