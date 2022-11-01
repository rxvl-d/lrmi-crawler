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

  private def downloadFile(startFileUrl: String) = IO {
    val outPath = localStartFilePath(startFileUrl)
    val pb = URL(startFileUrl) #> new File(outPath)
    pb.!!
    outPath
  }

  private def checkFile(startFileUrl: String) = IO {
    val localFile: String = localStartFilePath(startFileUrl)
    new File(localFile).exists()
  }

  private def localStartFilePath(startFileUrl: String) = {
    val name = startFileUrl.split("/").last
    val localFile = s"/tmp/$name"
    localFile
  }

  private def downloadFileIfNotExists(startFileUrl: String) = checkFile(startFileUrl).flatMap(e =>
    if (e) IO.pure(localStartFilePath(startFileUrl)) else downloadFile(startFileUrl))

  def startFileLines(s: String): Stream[IO, String] = fs2.io.readInputStream(
    IO({
      val bis = new BufferedInputStream(new FileInputStream(s))
      if (s.endsWith("gz")) new GZIPInputStream(bis)
      else bis
    }),
    1024
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

  def processFile(extract: Extract)(fileUrl: String): IO[Unit] = for {
    fileExists <- checkIfResultFileExists(fileUrl)
    _ <-
      if (fileExists) IO(System.err.println(s"Skipping $fileUrl because cached."))
      else extract(fileUrl).flatMap(writeToFile(fileUrl))
  } yield ()

  def processFileRepeat(extract: Extract)(fileUrl: String): IO[Unit] = processFile(extract)(fileUrl)
    .handleErrorWith(_ => processFile(extract)(fileUrl))
    .handleErrorWith(err =>
      IO(System.err.println(s"Skipping $fileUrl because couldn't parse. Error [$err]")))

  def extractCC(filePath: String): IO[List[(String, String, String)]] = {
    WARCParser.parse(
      gzipUrlToLines("https://data.commoncrawl.org/" ++ filePath)
    ).map(_.collect({
      case WARCResponse(url, LRMI(triples @ _ :: _)) => triples.map({
        case (v, o) => (url, v, o)
      })
    }).flatten)
  }

  def toTriple(line: String): (String, String, String) = {
    val groups = "<(.*)> <(.*)> <(.*)>".r.findAllIn(line).toList
    (groups(0), groups(1), groups(2))
  }

  def extractWDC(fileUrl: String): IO[List[(String, String, String)]] = {
    WDCParser.extractWDC(gzipUrlToLines(fileUrl))
  }

  def observeProgress(s: Stream[IO, String], total: Int): Stream[IO, String] =
    s.zipWithIndex.evalTap({case (_, i) => IO{
      System.err.println(s"$i/$total")
    }}).map(_._1)

  type Extract = String => IO[List[(String, String, String)]]
  def processFiles(extract: Extract)
                  (nCores: Int, nFiles: Option[Int],  fileFilter: Option[String])
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
      _ <- observeProgress(limited, totalFiles).parEvalMap(nCores)(processFileRepeat(extract)).compile.drain
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
  def gzipUrlToLines(spec: String): fs2.Stream[IO, String] = {
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
    val source = args.lift(1)
      .map(s => Source(s).getOrElse(throw new Exception(s"Unknown source $s")))
      .getOrElse(CommonCrawl) // cc / wdc
    val nFiles = args.lift(2).map(_.toInt)
    val fileFilter = args.lift(3)
    val extract = source match {
      case CommonCrawl => extractCC
      case WebDataCommons => extractWDC
    }
    val startFile = source match {
      case CommonCrawl => "https://data.commoncrawl.org/crawl-data/CC-MAIN-2022-33/warc.paths.gz"
      case WebDataCommons => "http://webdatacommons.org/structureddata/2021-12/files/file.list"
    }
    for {
      fileList <- downloadFileIfNotExists(startFile).map(startFileLines)
      _ <- processFiles(extract)(nCores, nFiles, fileFilter)(fileList)
    } yield ExitCode.Success
  }
}

sealed trait Source
case object Source {
  def apply(asStr: String): Option[Source] = asStr match {
    case "cc" => Some(CommonCrawl)
    case "wdc" => Some(WebDataCommons)
    case _ => None
  }
}
case object CommonCrawl extends Source
case object WebDataCommons extends Source