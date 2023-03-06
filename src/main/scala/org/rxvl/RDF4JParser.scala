package org.rxvl

import cats.Applicative
import cats.effect.{IO, Resource}
import com.opencsv.CSVWriter
import org.eclipse.rdf4j.model.Statement
import org.eclipse.rdf4j.model.Resource as RDF4JResource
import org.eclipse.rdf4j.model.impl.LinkedHashModel
import org.eclipse.rdf4j.query.QueryResults
import org.eclipse.rdf4j.rio.ParserConfig
import org.eclipse.rdf4j.rio.helpers.StatementCollector
import org.eclipse.rdf4j.rio.nquads.NQuadsParser

import java.io.FileInputStream
import java.util.function.Consumer
import java.util.zip.GZIPInputStream
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.*
import scala.util.Try


object RDF4JParser {
  def process(filePath: String): IO[Unit] = {
    val inFile = Resource.make(
      IO(new GZIPInputStream(new FileInputStream(filePath))))(
      is => IO(is.close()))

    val outFile = Resource.make(
      IO(new CSVWriter(new java.io.PrintWriter(new java.io.File(filePath
        .replace("webdatacommons", "webdatacommons/out/")
        .replace(".gz", ".nq"))))))(
      pw => IO(pw.close()))

    val subjects = inFile.use(f => IO {
      val parser = new NQuadsParser()
      parser.setStopAtFirstError(false)
      val res = QueryResults.parseGraphBackground(f, "http://example.org/", parser)
      val subjects = scala.collection.mutable.Set[RDF4JResource]()
      var lrmiStatements: Long = 0
      var total: Long = 0
      while (logError(Try(res.hasNext)).getOrElse(false)) {
        total += 1
        val st = res.next()
        if (WDCParser.lrmiPropURLs.contains(st.getPredicate.stringValue())) {
          subjects += st.getSubject
          lrmiStatements += 1
        }
        total += 1
      }
      (lrmiStatements, total, subjects)
    })

    subjects.flatMap { case (lrmiStatements, total, subs) =>
      inFile.use(f => outFile.use(out => IO {
        val parser = new NQuadsParser()
        parser.setStopAtFirstError(false)
        val res = QueryResults.parseGraphBackground(f, "http://example.org/", parser)
        var lrmiSubjects: Long = 0
        while (Try(res.hasNext).getOrElse(false)) {
          val st = res.next()
          if (subs.contains(st.getSubject)) {
            lrmiSubjects += 1
            out.writeNext(Array(st.getSubject.stringValue(),
              st.getPredicate.stringValue(),
              st.getObject.stringValue(),
              st.getContext.stringValue()))
          }
        }
        println(s"Total statements: $total . " +
          s"LRMI statements: $lrmiStatements . " +
          s"LRMI Subjects statements: $lrmiSubjects .")
      }))
    }
  }

  def toTSV(statement: Statement): String = {
    val separator = "!_SEP_!"
    val subject = statement.getSubject.stringValue()
    val predicate = statement.getPredicate.stringValue()
    val obj = statement.getObject.stringValue()
    val graph = statement.getContext.stringValue()
    subject + separator + predicate + separator + obj + separator + graph
  }
  def writeToFile(value: List[Statement], filePath: String): IO[Unit] = {
    val file = Resource.make(
      IO(new java.io.PrintWriter(new java.io.File(filePath))))(
      pw => IO(pw.close()))
    file.use { f => IO {
      value.map(toTSV).foreach(f.println)
    }}
  }

  def logError[T](trie: Try[T]): Try[T] = {
    trie.recoverWith {
      case e: Exception =>
        System.err.println(e)
        trie
    }
  }
}
