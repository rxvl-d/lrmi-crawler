package org.rxvl

import org.parboiled2._

object NQUADParser {
  case class NQStatement(s: String, p: String, v: String, url: String)

  case class Error(msg: String)

  /** Parses the given input into a [[CsvFile]] or an [[Error]] instance.
   */
  def apply(input: ParserInput): Either[Error, NQStatement] = {
    import Parser.DeliveryScheme.Either
    val parser = new NQUADParser(input)
    parser.statement.run().left.map(error => Error(parser.formatError(error)))
  }
}

class NQUADParser(val input: ParserInput) extends Parser {

  import NQUADParser._

  def SPACE = rule(zeroOrMore(' '))

  def statement = rule(
    sub ~ SPACE ~ pred ~ SPACE ~ obj ~ SPACE ~ url ~ SPACE ~ '.' ~ SPACE ~ EOI
      ~> NQStatement.apply _)

  def sub = rule(bnode | url)

  def pred = rule(url)

  def obj = rule(bnode | url | literal)

  def url = rule(capture('<' ~ oneOrMore(noneOf(" \n>")) ~ '>'))

  def literal = rule(capture(zeroOrMore(ANY ~ !(SPACE ~ url ~ SPACE ~ '.' ~ EOI)) ~ ANY))

  def bnode = rule(capture('_' ~ ':' ~ oneOrMore(CharPredicate.AlphaNum)))
}

