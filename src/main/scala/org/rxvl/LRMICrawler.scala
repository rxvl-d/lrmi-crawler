package org.rxvl

import cats.effect.{ExitCode, IO, IOApp}
import fs2.io.file.{Files, Path}
import fs2.{Stream, text}
import org.eclipse.rdf4j.model.{IRI, Resource, Value}
import org.http4s.ember.client.EmberClientBuilder

import java.io.{BufferedInputStream, BufferedReader, File, FileInputStream, FileOutputStream}
import java.net.URL
import java.nio.file.Paths
import java.util.zip.GZIPInputStream
import scala.sys.process.*
import scala.concurrent.duration.*

object LRMICrawler extends IOApp {

  private def downloadFile = IO {
    val outPath = "/tmp/warc.paths.gz"
    val pb = URL("https://data.commoncrawl.org/crawl-data/CC-MAIN-2022-33/warc.paths.gz") #> new File(outPath)
    pb.!!
    outPath
  }

  private def checkFile = IO {new File("/tmp/warc.paths.gz").exists()}

  private def downloadFileIfNotExists = checkFile.flatMap(e =>
    if (e) IO.pure("/tmp/warc.paths.gz") else downloadFile)

  def gis(s: String): Stream[IO, String] = fs2.io.readInputStream(
    IO(new GZIPInputStream(new BufferedInputStream(new FileInputStream(s)))),
    1000
  ).through(text.utf8.decode).through(text.lines)

  def countLines(in: Stream[IO, String]): IO[Int] = in.fold(0)((n: Int, _: String) => n + 1)
    .compile.last.map(_.get)

  def checkIfResultFileExists(fileUrl: String): IO[Boolean] =
    IO(new File(cacheFile(fileUrl)).exists())

  def writeToFile(fileUrl: String)
                 (out: List[(String, String, String)]): IO[Unit] = for {
    _ <- Stream
      .evals(IO.pure(out))
      .map({case (s,v,p) => s"$s,$v,$p\n"})
      .through(text.utf8.encode)
      .through(fs2.io.writeOutputStream[IO](
        IO(new FileOutputStream(cacheFile(fileUrl)))))
      .compile
      .drain
  } yield ()

  private def cacheFile(fileUrl: String) = {
    val sanitized = fileUrl.replace("/", "-")
    s"./lrmi-crawler-out/$sanitized.out"
  }

  def processFile(fileUrl: String): IO[Unit] = for {
    fileExists <- checkIfResultFileExists(fileUrl)
    _ <-
      if (fileExists) IO(System.err.println(s"Skipping $fileUrl because cached."))
      else extract(fileUrl).flatMap(writeToFile(fileUrl))
  } yield ()

  def processFileRepeat(fileUrl: String): IO[Unit] = processFile(fileUrl)
    .handleErrorWith(_ => processFile(fileUrl))
    .handleErrorWith(err =>
      IO(System.err.println(s"Skipping $fileUrl because couldn't parse. Error [$err]")))

  def extract(fileUrl: String): IO[List[(String, String, String)]] = {
    WARCParser.parse(
      fileUrl, warcLines("https://data.commoncrawl.org/" ++ fileUrl)
    ).map(_.collect({
      case WARCResponse(url, LRMI(triples @ _ :: _)) => triples.map({
        case (v, o) => (url, v, o)
      })
    }).flatten)
  }

  def observeProgress(s: Stream[IO, String], total: Int): Stream[IO, String] =
    s.zipWithIndex.evalTap({case (_, i) => IO{
      System.err.println(s"$i/$total")
    }}).map(_._1)

  def processFiles(nCores: Int, nFiles: Option[Int],  fileFilter: Option[String])
                  (files: Stream[IO, String]): IO[Unit] = {
    val filtered = fileFilter match {
      case Some(ff) => files.filter(_.contains(ff))
      case None => files
    }
    val limited  = nFiles match {
      case Some(nf) => filtered.take(nf)
      case None => filtered
    }

    for {
      totalFiles <- countLines(limited)
      _ <- observeProgress(limited, totalFiles).parEvalMap(nCores)(processFileRepeat).compile.drain
    } yield ()

  }

  private def retryWithBackoff[A](ioa: IO[A], initialDelay: FiniteDuration, maxRetries: Int): IO[A] = {

    ioa.handleErrorWith { error =>
      if (maxRetries > 0)
        IO.sleep(initialDelay) *> retryWithBackoff(ioa, initialDelay * 2, maxRetries - 1)
      else
        IO.raiseError(error)
    }
  }
  def warcLines(spec: String): fs2.Stream[IO, String] = {
    val inputStream = retryWithBackoff(IO({
      val is = new URL(spec).openConnection.getInputStream
      val bis = new BufferedInputStream(is)
      val gis = new GZIPInputStream(bis)
      gis
    }), 5.seconds, 5)
    fs2.io.readInputStream[IO](inputStream, 4096, closeAfterUse=true)
      .through(fs2.text.utf8.decode)
      .through(fs2.text.lines)
  }

  final def run(args: List[String]): IO[ExitCode] = {
    val nCores = args.head.toInt
    val nFiles = args.lift(1).map(_.toInt)
    val fileFilter = args.lift(2)
    for {
      fileList <- downloadFileIfNotExists.map(gis)
//      totalFiles <- countLines(fileList)
      _ <- processFiles(nCores, nFiles, fileFilter)(fileList)
    } yield ExitCode.Success
  }
}


