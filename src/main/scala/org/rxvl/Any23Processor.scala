package org.rxvl

import cats.Applicative
import cats.effect.{IO, MonadCancel, Resource as catsResource}
import org.apache.any23.Any23
import org.apache.any23.configuration.Settings
import org.apache.any23.extractor.{ExtractionContext, ExtractorGroup}
import org.apache.any23.extractor.rdf.NQuadsExtractorFactory
import org.apache.any23.rdf.RDFUtils
import org.apache.any23.writer.{FormatWriter, TripleFormat, TripleHandlerException, TripleWriterHandler, WriterSettings}
import org.eclipse.rdf4j.model.{IRI, Resource, Value}
import org.eclipse.rdf4j.rio.{RDFFormat, RDFHandlerException, RDFWriter, RDFWriterFactory, WriterConfig}
import org.apache.any23.source.ByteArrayDocumentSource
import org.eclipse.rdf4j.rio.helpers.NTriplesWriterSettings

import java.io.*
import java.nio.charset.Charset
import java.util.{Locale, Optional}
import java.util.zip.GZIPInputStream
import scala.jdk.CollectionConverters.*
object Any23Processor {
  def process(filePath: String): IO[Unit] = {
    val file = catsResource.make(
      IO(new GZIPInputStream(new FileInputStream(filePath))))(
      is => IO(is.close()))

    val tripleHandler = catsResource.make(
      IO(new NQuadsWriter(System.out, Settings.of)))(
      w => IO(w.close())
    )
    Applicative[({type l[a] = catsResource[IO, a]})#l].tuple2(file, tripleHandler).use {
      case (f, handler) => IO {

        val source = new ByteArrayDocumentSource(f,
          "http://example.com/",
          "application/n-quads")

        val extractor = new NQuadsExtractorFactory()
        val runner = new Any23(new ExtractorGroup(List(extractor).asJavaCollection))
        runner.extract(source, handler)
      }
    }
  }

}



/**
 * A {@link TripleHandler} that writes triples to a Sesame {@link org.eclipse.rdf4j.rio.RDFWriter}, eg for serialization
 * using one of Sesame's writers.
 *
 * @author Richard Cyganiak (richard@cyganiak.de)
 * @author Michele Mostarda (mostarda@fbk.eu)
 * @author Hans Brende (hansbrende@apache.org)
 */
class NQuadsWriter(os: OutputStream, settings: Settings) extends TripleWriterHandler with FormatWriter {

  private val writerFactory = new org.eclipse.rdf4j.rio.nquads.NQuadsWriterFactory()
  private val rdfFormat: RDFFormat = writerFactory.getRDFFormat
  private val format: TripleFormat = TripleFormat.of(
    rdfFormat.getName,
    rdfFormat.getMIMETypes,
    rdfFormat.getCharset,
    rdfFormat.getFileExtensions,
    if (rdfFormat.getStandardURI == null) null else rdfFormat.getStandardURI.stringValue,
    capabilities(rdfFormat))
  private var annotated: Boolean = false
  private var writerStarted: Boolean = false;
  private val charset = format.getCharset

  val (w, out) = if (!charset.isPresent) {
    (writerFactory.getWriter(os), os)
  } else {
    // use buffered writer if format supports encoding
    val buf = new BufferedWriter(new OutputStreamWriter(os, charset.get))
    (writerFactory.getWriter(buf), buf)
  }
  private var _writer: RDFWriter = w;

  configure(w.getWriterConfig, settings)

  def configure(config: WriterConfig, settings: Settings): Unit = {
    config.set(NTriplesWriterSettings.ESCAPE_UNICODE, settings.get(WriterSettings.PRINT_ASCII))
  }
  private def capabilities(format: RDFFormat) =
    if (format.supportsContexts)
      if (format.supportsNamespaces) TripleFormat.QUADS_AND_NAMESPACES
      else TripleFormat.QUADS
    else if (format.supportsNamespaces) TripleFormat.TRIPLES_AND_NAMESPACES
    else TripleFormat.TRIPLES

  @throws[TripleHandlerException]
  def writer = {
    val w = _writer
    if (w == null) throw new TripleHandlerException("writer has been closed!")
    if (!writerStarted) {
      writerStarted = true
      try w.startRDF()
      catch {
        case e: RDFHandlerException =>
          throw new TripleHandlerException("Error while starting document", e)
      }
    }
    w
  }

  /**
   * If <code>true</code> then the produced <b>RDF</b> is annotated with the extractors used to generate the specific
   * statements.
   *
   * @return the annotation flag value.
   */
  override def isAnnotated: Boolean = annotated

  /**
   * Sets the <i>annotation</i> flag.
   *
   * @param f
   * If <code>true</code> then the produced <b>RDF</b> is annotated with the extractors used to generate
   * the specific statements.
   */
  override def setAnnotated(f: Boolean): Unit = {
    annotated = f
  }

  @throws[TripleHandlerException]
  override def startDocument(documentIRI: IRI): Unit = {
    handleComment("OUTPUT FORMAT: " + format)
  }

  @throws[TripleHandlerException]
  override def openContext(context: ExtractionContext): Unit = {
    handleComment("BEGIN: " + context)
  }

  @throws[TripleHandlerException]
  override def writeTriple(s: Resource, p: IRI, o: Value, g: Resource): Unit = {
    if (WDCParser.lrmiPropURLs.contains(p.stringValue())) {
      try writer.handleStatement(RDFUtils.quad(s, p, o, g))
      catch {
        case ex: RDFHandlerException =>
          throw new TripleHandlerException(String.format(Locale.ROOT, "Error while receiving triple: %s %s %s %s", s, p, o, g), ex)
      }
    }
  }

  @throws[TripleHandlerException]
  override def writeNamespace(prefix: String, uri: String): Unit = {
    try writer.handleNamespace(prefix, uri)
    catch {
      case ex: RDFHandlerException =>
        throw new TripleHandlerException(String.format(Locale.ROOT, "Error while receiving namespace: %s:%s", prefix, uri), ex)
    }
  }

  @throws[TripleHandlerException]
  override def closeContext(context: ExtractionContext): Unit = {
    handleComment("END: " + context)
  }

  @throws[TripleHandlerException]
  override def close(): Unit = {
    val writer = _writer
    if (writer == null)
      return ()
    try {
      if (!writerStarted) writer.startRDF()
      writer.endRDF() // calls flush()
    } catch {
      case e: RDFHandlerException =>
        throw new TripleHandlerException("Error closing writer", e)
    }
  }

  @throws[TripleHandlerException]
  override def endDocument(documentIRI: IRI): Unit = {
    try out.flush()
    catch {
      case e: IOException =>
        throw new TripleHandlerException("Error ending document", e)
    }
  }

  @throws[TripleHandlerException]
  private def handleComment(comment: String): Unit = {
    if (!annotated) return
      try writer.handleComment(comment)
      catch {
        case rdfhe: RDFHandlerException =>
          throw new TripleHandlerException("Error while handing comment.", rdfhe)
      }
  }
}

