package org.rxvl

import cats.effect.{ExitCode, IO, IOApp}
import fs2.io.file.{Files, Path}
import fs2.{Stream, text}
import org.http4s.ember.client.EmberClientBuilder

import java.io.{BufferedInputStream, BufferedReader, File, FileInputStream}
import java.net.URL
import java.nio.file.Paths
import java.util.zip.GZIPInputStream
import scala.sys.process.*

object Converter extends IOApp {

  def downloadFile() = IO {
    val outPath = "/tmp/warc.paths.gz"
    val pb = URL("https://data.commoncrawl.org/crawl-data/CC-MAIN-2022-33/warc.paths.gz") #> new File(outPath)
    pb.!!
    outPath
  }

  def gis(s: String): Stream[IO, String] = fs2.io.readInputStream(
    IO(new GZIPInputStream(new BufferedInputStream(new FileInputStream(s)))),
    1000
  ).through(text.utf8.decode).through(text.lines)

  def countLines(in: Stream[IO, String]): IO[Int] = in.fold(0)((n: Int, _: String) => n + 1)
    .compile.last.map(_.get)

  def print(in: Any): IO[Unit] = IO(println(in))

  def processFile(fileUrl: String): IO[Unit] = IO(println(s"Starting $fileUrl")).flatMap(_ => {
    val warcSegments = WARCParser.parse(warcLines("https://data.commoncrawl.org/" ++ fileUrl))
    warcSegments.flatMap(s => {
      if (s.nonEmpty) {
        IO(println(s"Found $s in $fileUrl"))
      }
      else IO.pure(())
    })
  })

  def processFiles(n: Int, nCores: Int)(files: Stream[IO, String]): IO[Unit] = {
    files.take(n).parEvalMap(nCores)(processFile).compile.drain
  }

  def warcLines(spec: String): fs2.Stream[IO, String] = {
    val inputStream = IO({
      val is = new URL(spec).openConnection.getInputStream
      val bis = new BufferedInputStream(is)
      val gis = new GZIPInputStream(bis)
      gis
    })
    fs2.io.readInputStream[IO](inputStream, 4096, closeAfterUse=true)
      .through(fs2.text.utf8.decode)
      .through(fs2.text.lines)
  }

  final def run(args: List[String]): IO[ExitCode] = {
    val nFiles = args.head.toInt
    val nCores = args(1).toInt
    downloadFile().map(gis).flatMap(processFiles(nFiles, nCores)).map(_ => ExitCode.Success)
  }

//  def run: IO[Unit] = WARCParser.parse(
//    "/home/rsebastian/Downloads/CC-MAIN-20220807150925-20220807180925-00009.warc").flatMap {
//    i => IO(println(i.count({case WARCResponse(_, l) => l.isDefined; case _ => false})))
//  }
}

case class ProcessedFile(name: String, warcSegments: List[WARCSegment])
