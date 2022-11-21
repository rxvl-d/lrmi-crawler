package org.rxvl

import cats.effect.unsafe.implicits.global

import java.io.{ByteArrayInputStream, StringBufferInputStream, StringReader}
import java.nio.charset.StandardCharsets


class WDCParserTest extends munit.FunSuite {
  test("WDCParser") {
    val toExtract =
      """<http://1000lifehacks.com/#organization> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://schema.org/Organization> <http://1000lifehacks.com/how-to-remember-stuff-in-the-morning/>   .
        |<http://1000lifehacks.com/#organization> <http://schema.org/educationalLevel> <http://1000lifehacks.com/#logo> <http://1000lifehacks.com/how-to-remember-stuff-in-the-morning/>   .
        |<http://1000lifehacks.com/#organization> <http://schema.org/logo> <http://1000lifehacks.com/#logo> <http://1000lifehacks.com/how-to-remember-stuff-in-the-morning/>   .
        |<http://1000lifehacks.com/#organization> <http://schema.org/name> "1000 Life Hacks" <http://1000lifehacks.com/how-to-remember-stuff-in-the-morning/>   .
        |<http://1000lifehacks.com/#organization> <http://schema.org/sameAs> <http://facebook.com/1000lifehacks> <http://1000lifehacks.com/how-to-remember-stuff-in-the-morning/>   .
        |<http://1000lifehacks.com/#organization> <http://schema.org/sameAs> <http://instagram.com/lifehack.ers> <http://1000lifehacks.com/how-to-remember-stuff-in-the-morning/>   .
        |<http://1000lifehacks.com/#organization> <http://schema.org/sameAs> <https://twitter.com/1000lifehacks> <http://1000lifehacks.com/how-to-remember-stuff-in-the-morning/>   .
        |<http://1000lifehacks.com/#organization> <http://schema.org/url> <http://1000lifehacks.com/> <http://1000lifehacks.com/how-to-remember-stuff-in-the-morning/>   .
        |<http://1000lifehacks.com/#logo> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://schema.org/ImageObject> <http://1000lifehacks.com/how-to-remember-stuff-in-the-morning/>   .
        |<http://1000lifehacks.com/#logo> <http://schema.org/caption> "1000 Life Hacks" <http://1000lifehacks.com/how-to-remember-stuff-in-the-morning/>   .
        |""".stripMargin
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
