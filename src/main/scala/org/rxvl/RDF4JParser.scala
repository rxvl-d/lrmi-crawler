package org.rxvl

import cats.effect.{IO, Resource}
import org.eclipse.rdf4j.model.Statement
import org.eclipse.rdf4j.model.impl.LinkedHashModel
import org.eclipse.rdf4j.rio.ParserConfig
import org.eclipse.rdf4j.rio.helpers.StatementCollector
import org.eclipse.rdf4j.rio.nquads.NQuadsParser

import java.io.FileInputStream
import java.util.function.Consumer
import java.util.zip.GZIPInputStream
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.*


object RDF4JParser {
  def process(filePath: String): IO[Unit] = {
    val file = Resource.make(
      IO(new GZIPInputStream(new FileInputStream(filePath))))(
      is => IO(is.close()))

    file.use { f => IO {
      val parser = new NQuadsParser()
      val model = new LinkedHashModel()
      parser.setRDFHandler(new StatementCollector(model))
      parser.setStopAtFirstError(false)
      parser.parse(f, "http://example.org/")
      val lrmiStatementsBuffer = new ListBuffer[Statement]()
      model.forEach((t: Statement) =>
        if (WDCParser.lrmiPropURLs.contains(t.getPredicate.stringValue())) lrmiStatementsBuffer.append(t)
      )
      val lrmiStatements = lrmiStatementsBuffer.toList
      val subjects = lrmiStatements.map(_.getSubject)
      val lrmiSubjectStatements = subjects.flatMap(s => model.filter(s, null, null).asScala.toList)

      println("Found " + lrmiStatements.size + " statements with LRMI predicates. " +
        lrmiSubjectStatements.size + " statements with LRMI subjects.  " +
        model.size() + " total statements in " + filePath)
      lrmiSubjectStatements
    }}.flatMap(
      writeToFile(_,
        filePath
          .replace("webdatacommons", "webdatacommons/out/")
          .replace(".gz", ".nq")))
  }

  def writeToFile(value: List[Statement], filePath: String): IO[Unit] = {
    val file = Resource.make(
      IO(new java.io.PrintWriter(new java.io.File(filePath))))(
      pw => IO(pw.close()))

    file.use { f => IO {
      value.foreach(f.println)
    }}
  }
}
