package org.rxvl

import cats.effect.IO

object WDCParser {
  def toTriple(line: String): (String, String, String) = {
    val segments = line.split("""\s+""")
    (segments(0), segments(1), segments(2))
  }

  def extractWDC(lines: fs2.Stream[IO, String]): IO[List[(String, String, String)]] = {
    lines
      .filter(_.stripLineEnd.nonEmpty)
      .map(toTriple)
      .filter(t => WARCParser.isRelevantTriple(t._2))
      .compile
      .toList
  }

}
