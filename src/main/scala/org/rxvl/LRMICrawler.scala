package org.rxvl

import cats.effect.{ExitCode, IO, IOApp}
import fs2.io.file.{Files, Path}
import fs2.{Stream, text}
import org.eclipse.rdf4j.model.{IRI, Resource, Value}
import org.http4s.ember.client.EmberClientBuilder
import org.joda.time.DateTime

import java.io.{BufferedInputStream, BufferedReader, File, FileFilter, FileInputStream, FileOutputStream}
import java.net.URL
import java.nio.file.Paths
import java.util.zip.GZIPInputStream
import scala.sys.process.*
import scala.concurrent.duration.*
import scala.util.Try

object LRMICrawler extends IOApp {

  private def downloadFile(startFileUrl: String) = IO {
    val outPath = localStartFilePath(startFileUrl)
    println(s"Writing to $outPath")
    val pb = URL(startFileUrl) #> new File(outPath)
    pb.!!
    outPath
  }

  private def checkFile(startFileUrl: String) = IO {
    val localFile: String = localStartFilePath(startFileUrl)
    println(s"Checking $localFile")
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
      .map({case (s,v,p,u) => s"$s,$v,$p,$u\n"})
      .through(text.utf8.encode)
      .through(fs2.io.writeOutputStream[IO](
        IO(new FileOutputStream(destFile))))
      .compile
      .drain
  } yield ()

  private def destFilePath(sourceFilePath: String) =
    sourceFilePath.replace("webdatacommons/", "webdatacommons/out/")

  def processFileIfNotCached(sourceFilePath: String): IO[Unit] = for {
    fileExists <- checkIfResultFileExists(sourceFilePath)
    _ <-
      if (fileExists) IO(System.err.println(s"Skipping $sourceFilePath because cached."))
      else
        for {
          is <- pathToStream(sourceFilePath)
          lrmiStatements <- WDCParser.extractWDCStream(is)
          _ <- writeToFile(destFilePath(sourceFilePath))(lrmiStatements)
        } yield ()
  } yield ()

  def processFile(sourceFilePath: String): IO[Unit] =
    processFileIfNotCached(sourceFilePath)
      .handleErrorWith(err =>
        IO(System.err.println(s"Skipping $sourceFilePath because couldn't parse. Error [$err]")))

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

  def processFiles(source: Source,
                   nCores: Int,
                   fileList: Seq[String],
                   startTime: DateTime,
                   nFiles: Option[Int],
                   fileFilter: Option[String]): IO[Unit] = {
    val filtered = fileFilter match {
      case Some(ff) => fileList.filter(_.contains(ff))
      case None => fileList
    }
    val limited  = nFiles match {
      case Some(nf) => filtered.take(nf)
      case None => filtered
    }

    val fileStream = fs2.Stream.evalSeq(IO(limited))

    val totalFiles = limited.size

    for {
      _ <- observeProgress(fileStream, totalFiles, startTime)
        .parEvalMap(nCores)(processFile).compile.drain
    } yield ()

  }
  def pathToStream(sourceFilePath: String): IO[GZIPInputStream] = {
    IO({
      val fis = new FileInputStream(new File(sourceFilePath))
      val bis = new BufferedInputStream(fis)
      new GZIPInputStream(bis)
    })
  }

  def getFiles(source: Source): IO[Seq[String]] = IO {
    val dirPath = s"/nfs/data/webdatacommons/${source.dirName}/"
    val sourceDir = new File(dirPath)
    val dataFiles = sourceDir.listFiles(new FileFilter {
      override def accept(pathname: File): Boolean = pathname.getName.endsWith(".gz")
    })
    dataFiles.toList.map(_.getPath)
  }
  final def run(args: List[String]): IO[ExitCode] = {
    if (args.head == "debug") {
      processFile(args(1)).map(_ => ExitCode.Success)
    } else {
      val nCores = args.head.toInt
      assert(Set("2019", "2020", "2021").contains(args(1)))
      val source = args(1) match {
        case "2019" => WebDataCommons19
        case "2020" => WebDataCommons20
        case "2021" => WebDataCommons21
      }
      val startTime = args.lift(2).flatMap(s => Try(DateTime.parse(s)).toOption).getOrElse(DateTime.now)
      val nFiles = args.lift(3).map(_.toInt)
      val fileFilter = args.lift(4)
      for {
        fileList <- getFiles(source)
        _ <- processFiles(source, nCores, fileList, startTime, nFiles, fileFilter)
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