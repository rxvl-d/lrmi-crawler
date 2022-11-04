package org.rxvl

import cats.effect.IO

object WDCParser {
  private def toQuad(line: String) = {
    val segments = line.split("""\s+""")
    (segments(0), segments(1), segments(2), segments(3))
  }

  def extractWDC(lines: fs2.Stream[IO, String]): IO[List[(String, String, String, String)]] = {
    lines
      .filter(_.stripLineEnd.nonEmpty)
      .map(toQuad)
      .filter(t => WARCParser.isRelevantTriple(t._2))
      .compile
      .toList
  }

}
