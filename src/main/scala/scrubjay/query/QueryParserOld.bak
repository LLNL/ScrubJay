package scrubjay.query

import scala.util.parsing.combinator._
import scala.util.matching.Regex
import scala.language.postfixOps
import scrubjay.datasetid.{ScrubJayField, ScrubJaySchema}
import scala.collection.mutable.ArrayBuffer
/*
TODO Find out which characters are allowed
TODO  multiple sets of quotes?
 */


case class QueryToSchema(var domains: Seq[String], val values: Seq[String]){

  override def toString = {
    "Domains: " + (domains mkString ",") +
      "\nValues: " + (values mkString ",")
  }


  def generateScrubJaySchema(): ScrubJaySchema = {
    val fields = ArrayBuffer[ScrubJayField]()

    //For every item in each domains/values list,
    //Generate a SJField and make an array of them then
    //Call ScrubJay on them
    for (domainElem <- domains) {
      fields += ScrubJayField(domain = true, dimension = domainElem)
    }

    for (valueElem <- values) {
      fields += ScrubJayField(domain = false, dimension = valueElem)
    }
    ScrubJaySchema(fields.toArray)

  }

}

class QueryParser extends RegexParsers {
  /*
  Keywords
    Define specific words that must be matched in the query.
  */
  def select: Parser[String] =  buildCaseInsensitiveRegex("select")
  def domains: Parser[String] = buildCaseInsensitiveRegex("domains")
  def values: Parser[String] = buildCaseInsensitiveRegex("values")

  /*
  Punctuation
    Help readability for more complex rules
  */
  def openParen: Parser[String] = "("
  def closeParen: Parser[String] = ")"
  def quote: Parser[String] = "\""
  def comma: Parser[String] = ","

  /*
  Retrieving each col/val name
    name         : Each col/val can be optionally quoted. Preference is given to quoted name
    nonQuotedName: Set of characters allowed in a non-quoted name.
    quotedName   : Defines the structure of a quoted name.
    noQuotes     : Rule to define a set of characters allowed in a quoted name (everything except quotes for now)
  */
  def name: Parser[String] = (quotedName | nonQuotedName)
  def nonQuotedName: Parser[String] = """[a-zA-Z0-9_]+""".r ^^ {_.toString}
  def quotedName: Parser[String] = quote ~ noQuotes ~ quote ^^ {
    case openQuote ~ noQuotes ~ closeQuote => noQuotes.toString
  }
  def noQuotes: Parser[String] = """[^\"]*""".r ^^ {_.toString}

  /*
  List processing
    list : Defines a pattern to match list
    items: Reads names of col/val into a list
  */
  def list: Parser[Seq[String]] = (openParen ~ items ~ closeParen) ^^ {
    case openParen ~ items ~ closeParen => {
      items
    }
  }
  def items: Parser[Seq[String]] = name ~ rep(comma ~ name) ^^ {
    case (name ~ rest) => rest.foldLeft(Seq(name)) {
      case (left, ("," ~ right)) => left :+ right
    }
  }


  //For generating ScrubJaySchema
  def queryToSchema: Parser[ScrubJaySchema] = select ~ domains ~ list ~ values ~ list ^^ {
    case wd1 ~ wd2 ~ lst1 ~ wd3 ~ lst2 => {
      val q = QueryToSchema(lst1, lst2)
      q.generateScrubJaySchema()
    }
  }

  def parseQuery(queryString: String): ScrubJaySchema = {
    parse(queryToSchema, queryString) match {
      case Success(matched, _) => matched
      case Failure(msg, _) => throw new Exception("Invalid Query.")
      case Error(msg, _) => throw new Exception("Error while parsing.")
    }
  }

  def buildCaseInsensitiveRegex(str: String): Regex = {
    ("""(?i)\Q""" + str + """\E""").r
  }


  //For testing just the parser
  def queryTest: Parser[QueryToSchema] = select ~ domains ~ list ~ values ~ list ^^ {
    case wd1 ~ wd2 ~ lst1 ~ wd3 ~ lst2 => {
      QueryToSchema(lst1, lst2)
    }
  }

  def parseQueryTesting(queryString: String): QueryToSchema = {
    parse(queryTest, queryString) match {
      case Success(matched, _) => matched
      case Failure(msg, _) => throw new Exception("Invalid Query.")
      case Error(msg, _) => throw new Exception("Error while parsing.")
    }
  }

}


