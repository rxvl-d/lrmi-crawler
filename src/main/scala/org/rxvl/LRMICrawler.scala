package org.rxvl

import cats.effect.{ExitCode, IO, IOApp}
import fs2.io.file.{Files, Path}
import fs2.{Stream, text}
import org.eclipse.rdf4j.model.{IRI, Resource, Value}
import org.http4s.ember.client.EmberClientBuilder
import org.joda.time.{DateTime, Duration}

import java.io.{BufferedInputStream, BufferedReader, File, FileFilter, FileInputStream, FileOutputStream}
import java.net.URL
import java.nio.file.Paths
import java.util.zip.GZIPInputStream
import scala.concurrent.TimeoutException
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
    val name = startFileUrl.replace("/", "-").replace(":", "")
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

  def checkIfResultFileExists(sourceFilePath: String): IO[Boolean] =
    IO(new File(destFilePath(sourceFilePath)).exists())

  def writeToFile(destFile: String)
                 (out: List[(String, String, String, String)]): IO[Unit] = for {
    _ <- Stream
      .evals(IO.pure(out))
      .map({case (s,v,p,u) => s"$s\t$v\t$p\t$u\n"})
      .through(text.utf8.encode)
      .through(fs2.io.writeOutputStream[IO](
        IO(new FileOutputStream(destFile))))
      .compile
      .drain
  } yield ()

  private def destFilePath(sourceFilePath: String) =
    sourceFilePath.replace("webdatacommons/", "webdatacommons/out/")

  def processFileIfNotCached(sourceFilePath: String): IO[Unit] = for {
    start <- IO(DateTime.now())
    fileExists <- checkIfResultFileExists(sourceFilePath)
    _ <-
      if (fileExists) IO(System.err.println(s"[${DateTime.now()}]Skipping $sourceFilePath because cached."))
      else
        for {
          lrmiStatements <- WDCParser.extractWDC(pathToStream(sourceFilePath))
          _ <- writeToFile(destFilePath(sourceFilePath))(lrmiStatements)
        } yield ()
    end <- IO(DateTime.now())
    durationSeconds = new Duration(start, end).getStandardSeconds
    _ <- IO(System.err.println(s"$sourceFilePath took $durationSeconds"))
  } yield ()

  def processFile(sourceFilePath: String): IO[Unit] =
    processFileIfNotCached(sourceFilePath)
      .handleErrorWith {
        case _: TimeoutException => IO(System.err.println(
          s"[${DateTime.now()}] $sourceFilePath timed out"))
        case err => IO(System.err.println(
          s"[${DateTime.now()}] Skipping $sourceFilePath because couldn't parse. Error [$err]"))
      }


  def observeProgress(s: Stream[IO, String],
                      total: Int,
                      startTime: DateTime): Stream[IO, String] =
    s.zipWithIndex.evalTap({case (fn, i) => IO{
      val duration = new org.joda.time.Duration(startTime, DateTime.now())
      val timeSoFar = duration.toStandardSeconds.getSeconds.toFloat
      val timeForOne = timeSoFar / i
      val timeForAll = timeForOne * total
      val timeLeft = timeForAll - timeSoFar
      System.out.println(s"[${DateTime.now()}] $i/$total [$fn] Estimated to be done in: ${timeLeft / 60 / 60} hours.")
    }}).map(_._1)

  def processFiles(nCores: Int,
                   fileList: Seq[String],
                   startTime: DateTime): IO[Unit] = {
    val fileStream = fs2.Stream.evalSeq(IO(fileList))

    val totalFiles = fileList.size

    for {
      _ <- observeProgress(fileStream, totalFiles, startTime)
        .parEvalMap(nCores)(RDF4JParser.process).compile.drain
    } yield ()

  }
  def pathToStream(sourceFilePath: String): Stream[IO, String] = {
    val gis = IO({
      val fis = new FileInputStream(new File(sourceFilePath))
      val bis = new BufferedInputStream(fis)
      new GZIPInputStream(bis)
    })
    fs2.io.readInputStream[IO](gis, 4096, closeAfterUse = true)
      .through(fs2.text.utf8.decode)
      .through(fs2.text.lines)
  }

  def getFiles(source: Source): IO[Seq[String]] = IO {
    val dirPath = s"${System.getenv("WDC_DIR")}/${source.dirName}/"
    val sourceDir = new File(dirPath)
    val dataFiles = sourceDir.listFiles(new FileFilter {
      override def accept(pathname: File): Boolean = pathname.getName.endsWith(".gz")
    })
    dataFiles.toList.map(_.getPath)
  }
  final def run(args: List[String]): IO[ExitCode] = {
    if (args.head == "debug") {
      processFile(args(1)).map(_ => ExitCode.Success)
    }
    else if (args.head == "jena") {
      JenaProcessor.processFileJena(args(1)).map(_ => ExitCode.Success)
    }
    else if (args.head == "rdf4j") {
      RDF4JParser.process(args(1)).map(_ => ExitCode.Success)
    }
    else if (args.head == "any23") {
      Any23Processor.process(args(1)).map(_ => ExitCode.Success)
    } else {
      val nCores = args.head.toInt
      assert(Set("2019", "2020", "2021").contains(args(1)))
      val source = args(1) match {
        case "2019" => WebDataCommons19
        case "2020" => WebDataCommons20
        case "2021" => WebDataCommons21
      }
      val startTime = args.lift(2).flatMap(s => Try(DateTime.parse(s)).toOption).getOrElse(DateTime.now)
      for {
        fileList <- getFiles(source)
        _ <- processFiles(nCores, fileList, startTime)
      } yield ExitCode.Success
    }
  }
}

sealed trait Source {
  def dirName: String
}
case object WebDataCommons21 extends Source {
  val dirName: String = "2021"
}
case object WebDataCommons20 extends Source {
  val dirName: String = "2020"
}
case object WebDataCommons19 extends Source {
  val dirName: String = "2019"
}