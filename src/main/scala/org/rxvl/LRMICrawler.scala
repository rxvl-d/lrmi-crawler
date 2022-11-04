package org.rxvl

import cats.effect.{ExitCode, IO, IOApp}
import fs2.io.file.{Files, Path}
import fs2.{Stream, text}
import org.eclipse.rdf4j.model.{IRI, Resource, Value}
import org.http4s.ember.client.EmberClientBuilder
import org.joda.time.DateTime

import java.io.{BufferedInputStream, BufferedReader, File, FileInputStream, FileOutputStream}
import java.net.URL
import java.nio.file.Paths
import java.util.zip.GZIPInputStream
import scala.sys.process.*
import scala.concurrent.duration.*
import scala.util.Try

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
    val name = startFileUrl.replace("/", "-").replace(":", "").last
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

  def checkIfResultFileExists(label: String, fileUrl: String): IO[Boolean] =
    IO(new File(cacheFile(label, fileUrl)).exists())

  def writeToFile(label: String, fileUrl: String)
                 (out: List[(String, String, String, String)]): IO[Unit] = for {
    _ <- Stream
      .evals(IO.pure(out))
      .map({case (s,v,p,u) => s"$s,$v,$p,$u\n"})
      .through(text.utf8.encode)
      .through(fs2.io.writeOutputStream[IO](
        IO(new FileOutputStream(cacheFile(label, fileUrl)))))
      .compile
      .drain
  } yield ()

  private def cacheFile(label: String, fileUrl: String) = {
    val sanitized = fileUrl.replace("/", "-")
    s"./lrmi-crawler-out-$label/$sanitized.out"
  }

  def processFile(extract: Extract)(label: String, fileUrl: String): IO[Unit] = for {
    fileExists <- checkIfResultFileExists(label, fileUrl)
    _ <-
      if (fileExists) IO(System.err.println(s"Skipping $fileUrl because cached."))
      else extract(fileUrl).flatMap(writeToFile(label, fileUrl))
  } yield ()

  def processFileRepeat(label: String, extract: Extract)(fileUrl: String): IO[Unit] = processFile(extract)(label, fileUrl)
    .handleErrorWith(_ => processFile(extract)(label, fileUrl))
    .handleErrorWith(err =>
      IO(System.err.println(s"Skipping $fileUrl because couldn't parse. Error [$err]")))

  def extractCC(filePath: String): IO[List[(String, String, String, String)]] = {
    WARCParser.parse(
      gzipUrlToLines("https://data.commoncrawl.org/" ++ filePath)
    ).map(_.collect({
      case WARCResponse(url, LRMI(triples @ _ :: _)) => triples.map({
        case (v, o) => (url, v, o, url)
      })
    }).flatten)
  }

  def toTriple(line: String): (String, String, String) = {
    val groups = "<(.*)> <(.*)> <(.*)>".r.findAllIn(line).toList
    (groups(0), groups(1), groups(2))
  }

  def extractWDC(fileUrl: String): IO[List[(String, String, String, String)]] = {
    WDCParser.extractWDC(gzipUrlToLines(fileUrl.replace("http", "https")))
  }

  def observeProgress(s: Stream[IO, String],
                      total: Int,
                      startTime: DateTime): Stream[IO, String] =
    s.zipWithIndex.evalTap({case (_, i) => IO{
      val duration = new org.joda.time.Duration(startTime, DateTime.now())
      val timeSoFar = duration.toStandardSeconds.getSeconds.toFloat
      val timeForOne = timeSoFar / i
      val timeForAll = timeForOne * total
      val timeLeft = timeForAll - timeSoFar
      System.err.println(s"$i/$total. Estimated to be done in: ${timeLeft / 60 / 60} hours.")
    }}).map(_._1)

  type Extract = String => IO[List[(String, String, String, String)]]
  def processFiles(extract: Extract)
                  (label: String,
                   nCores: Int,
                   startTime: DateTime,
                   nFiles: Option[Int],
                   fileFilter: Option[String])
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
      _ <- observeProgress(limited, totalFiles, startTime)
        .parEvalMap(nCores)(processFileRepeat(label, extract)).compile.drain
    } yield ()

  }

  private def retryWithBackoff[A](ioa: IO[A],
                                  initialDelay: FiniteDuration,
                                  maxRetries: Int,
                                  label: String): IO[A] = {

    ioa.handleErrorWith { error =>
      if (maxRetries > 0)
        IO.sleep(initialDelay) *>
          IO(System.err.println(s"Retrying $label because [$error]")) *>
          retryWithBackoff(ioa, initialDelay * 2, maxRetries - 1, label)
      else
        IO.raiseError(error)
    }
  }
  def gzipUrlToLines(spec: String): fs2.Stream[IO, String] = {
    val inputStream = retryWithBackoff(IO({
      val connection = new URL(spec).openConnection
      val is = connection.getInputStream
      val bis = new BufferedInputStream(is)
      val gis = new GZIPInputStream(bis)
      gis
    }), 5.seconds, 5, spec)
    fs2.io.readInputStream[IO](inputStream, 4096, closeAfterUse=true)
      .through(fs2.text.utf8.decode)
      .through(fs2.text.lines)
  }

//  final def run(args: List[String]): IO[ExitCode] =
//    gzipUrlToLines(args.head.replace("http", "https"))
//      .take(10)
//      .evalMap(s => IO(println(s)))
//      .compile
//      .drain
//      .map(_ => ExitCode.Success)
  final def run(args: List[String]): IO[ExitCode] = {
    val nCores = args.head.toInt
    val source = args.lift(1)
      .map(s => Source(s).getOrElse(throw new Exception(s"Unknown source $s")))
      .getOrElse(CommonCrawl) // cc / wdc
    val startTime = args.lift(2).flatMap(s => Try(DateTime.parse(s)).toOption).getOrElse(DateTime.now)
    val nFiles = args.lift(3).map(_.toInt)
    val fileFilter = args.lift(4)
    val extract = source match {
      case CommonCrawl => extractCC
      case _: WebDataCommons => extractWDC
    }
    val startFile = source match {
      case CommonCrawl => "https://data.commoncrawl.org/crawl-data/CC-MAIN-2022-33/warc.paths.gz"
      case WebDataCommons21 => "http://webdatacommons.org/structureddata/2021-12/files/file.list"
      case WebDataCommons20 => "http://webdatacommons.org/structureddata/2020-12/files/file.list"
      case WebDataCommons19 => "http://webdatacommons.org/structureddata/2019-12/files/file.list"
    }
    val outLabel = source match {
      case CommonCrawl => "cc"
      case WebDataCommons21 => "wdc21"
      case WebDataCommons20 => "wdc20"
      case WebDataCommons19 => "wdc19"
    }
    for {
      fileList <- downloadFileIfNotExists(startFile).map(startFileLines)
      _ <- processFiles(
        extract)(
        outLabel, nCores, startTime, nFiles, fileFilter)(
        fileList)
    } yield ExitCode.Success
  }
}

sealed trait Source
case object Source {
  def apply(asStr: String): Option[Source] = asStr match {
    case "cc" => Some(CommonCrawl)
    case "wdc19" => Some(WebDataCommons19)
    case "wdc20" => Some(WebDataCommons20)
    case "wdc21" => Some(WebDataCommons21)
    case _ => None
  }
}
case object CommonCrawl extends Source

sealed trait WebDataCommons extends Source
case object WebDataCommons21 extends WebDataCommons
case object WebDataCommons20 extends WebDataCommons
case object WebDataCommons19 extends WebDataCommons