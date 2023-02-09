package org.rxvl

import cats.effect.{IO, Resource}
import org.apache.jena.graph.Triple
import org.apache.jena.riot.system.{ErrorHandler, ErrorHandlerFactory}
import org.apache.jena.riot.{RDFLanguages, RDFParser}
import org.apache.jena.sparql.core.Quad

import java.io.FileInputStream
import java.util.function.Predicate
import java.util.zip.GZIPInputStream
import scala.jdk.CollectionConverters
object JenaProcessor {

  val lrmiTerm: Predicate[Quad] = t =>
    WDCParser.lrmiPropURLs.contains(t.getPredicate.getName)
  def processFileJena(filePath: String): IO[Unit] ={
    val file = Resource.make(
      IO(new GZIPInputStream(new FileInputStream(filePath))))(
      f => IO(f.close()))
    file.use { f =>
      IO(RDFParser
        .create()
        .source(f)
        .lang(RDFLanguages.NQ)
        .errorHandler(new ErrorHandler {
          override def warning(message: String, line: Long, col: Long): Unit = ()

          override def error(message: String, line: Long, col: Long): Unit = ()

          override def fatal(message: String, line: Long, col: Long): Unit = ()
        })
        .toDataset
        .asDatasetGraph()
        .stream()
        .filter(lrmiTerm)
        .count()
      ).flatMap(s => IO(println(s"found $s LRMI triples")))
    }
  }
}
