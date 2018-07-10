package scrubjay.query

import scala.util.parsing.combinator._
import scala.util.matching.Regex
import scala.language.postfixOps
import scrubjay.datasetid.{ScrubJayField, ScrubJaySchema, ScrubJayUnitsField}

import scala.collection.mutable.ArrayBuffer
/*
TODO Find out which characters are allowed
TODO  multiple sets of quotes?
 */


case class Member(dim: String, unit: Option[ScrubJayUnitsField])


class QueryParser extends RegexParsers {
  /*
  Keywords
    Define specific words that must be matched in the query.
  */
  def select: Parser[String] =  buildCaseInsensitiveRegex("select")
  def domain: Parser[String] = buildCaseInsensitiveRegex("domain")
  def value: Parser[String] = buildCaseInsensitiveRegex("value")
  def units: Parser[String] = buildCaseInsensitiveRegex("units")
  def dimension: Parser[String] = buildCaseInsensitiveRegex("dim")

  def unitName: Parser[String] = buildCaseInsensitiveRegex("name")
  def elementType: Parser[String] = buildCaseInsensitiveRegex("elementType")
  def aggregator: Parser[String] = buildCaseInsensitiveRegex("aggregator")
  def interpolator: Parser[String] = buildCaseInsensitiveRegex("interpolator")
  def subunits: Parser[String] = buildCaseInsensitiveRegex("subunits")

  /*
  Punctuation
    Help readability for more complex rules
  */
  def openParen: Parser[String] = "("
  def closeParen: Parser[String] = ")"
  def quote: Parser[String] = "\""
  def comma: Parser[String] = ","
  def colon: Parser[String] = ":"
  /*
  Retrieving each col/val name
    name         : Each col/val can be optionally quoted. Preference is given to quoted name
    nonQuotedName: Set of characters allowed in a non-quoted name.
    quotedName   : Defines the structure of a quoted name.
    noQuotes     : Rule to define a set of characters allowed in a quoted name (everything except quotes for now)
  */
  def name: Parser[String] = (quotedName | nonQuotedName)
  def nonQuotedName: Parser[String] = """[a-zA-Z0-9_*.]+""".r ^^ {_.toString}
  def quotedName: Parser[String] = quote ~ noQuotes ~ quote ^^ {
    case openQuote ~ noQuotes ~ closeQuote => noQuotes.toString
  }
  def noQuotes: Parser[String] = """[^\"]*""".r ^^ {_.toString}

  /*
  List processing
    list : Defines a pattern to match list
    items: Recursively reads names of col/val into a list
  */
  def list: Parser[Seq[String]] = (openParen ~ items ~ closeParen) ^^ {
    case openParen ~ items ~ closeParen => {
      items
    }
  }
  def items: Parser[Seq[String]] = name ~ rep(comma ~ name) ^^ {
    case (name ~ list) => list.foldLeft(Seq(name)) {
      case (left, ("," ~ right)) => left :+ right
    }
  }

  /*
  Specify each domain and value
     fields: VALUE(DIM(flops)), VALUE(DIM(time), UNITS(seconds)), DOMAIN(DIM(time), UNITS(timestamp))
     fieldPattern: DOMAIN(_________________)
     fieldMembers: DIM(time), UNITS(timestamp)
   */

  def fields: Parser[ArrayBuffer[ScrubJayField]] = fieldPattern ~ rep(comma ~ fieldPattern) ^^ {
    case (field ~ rest) => rest.foldLeft(ArrayBuffer(field)) {
      case (field, ("," ~ rest)) => field :+ rest
    }
  }

  def fieldPattern: Parser[ScrubJayField] = (fieldType ~ openParen ~ fieldMembers ~ closeParen) ^^ {
    case fieldType ~ openParen ~ fieldMembers ~ closeParen => {
      val unit: ScrubJayUnitsField = fieldMembers.unit.getOrElse(ScrubJayUnitsField.unknown)
      var domain: Boolean = false
      if (fieldType.toLowerCase.equals("domain")) {
        domain = true
      }
      ScrubJayField(domain = domain, dimension = fieldMembers.dim, units = unit)
    }
  }
  def fieldType: Parser[String] = (domain | value)
  def fieldMembers: Parser[Member] = dimensionMember ~ opt(comma) ~ opt(unitsMember) ^^ {
    case dimensionMember ~ comma ~ unitsMember => {
      Member(dimensionMember, unitsMember)
    }

  }

  def dimensionMember: Parser[String] = (dimension ~ openParen ~ name ~ closeParen) ^^ {
    case dimension ~ openParen ~ name ~ closeParen =>
      name
  }

  //Work in progress
  def unitsMember: Parser[ScrubJayUnitsField] = (units ~ openParen ~
    nameArg.? ~ comma.? ~
    elementTypeArg.? ~ comma.? ~
    aggregatorArg.? ~ comma.? ~
    interpolatorArg.? ~ comma.? ~
    subunitsArg.? ~
    closeParen) ^^ {
    case units ~ openParen ~
      nameArg ~ comma1 ~
      elementTypeArg ~ comma2 ~
      aggregatorArg ~ comma3 ~
      interpolatorArg ~ comma4 ~
      subunitsArg ~
      closeParen
      => ScrubJayUnitsField(
        nameArg.getOrElse(ScrubJayUnitsField.unknown.name),
        elementTypeArg.getOrElse(ScrubJayUnitsField.unknown.elementType),
        aggregatorArg.getOrElse(ScrubJayUnitsField.unknown.aggregator),
        interpolatorArg.getOrElse(ScrubJayUnitsField.unknown.interpolator),
        subunitsArg.getOrElse(ScrubJayUnitsField.unknown.subUnits)
      )
  }

  def nameArg: Parser[String] = (unitName ~ openParen ~ name ~ closeParen) ^^ {
    case dimension ~ openParen ~ name ~ closeParen =>
      name
  }

  def elementTypeArg: Parser[String] = (elementType ~ openParen ~ name ~ closeParen) ^^ {
    case dimension ~ openParen ~ name ~ closeParen =>
      name
  }

  def aggregatorArg: Parser[String] = (aggregator ~ openParen ~ name ~ closeParen) ^^ {
    case dimension ~ openParen ~ name ~ closeParen =>
      name
  }

  def interpolatorArg: Parser[String] = (interpolator ~ openParen ~ name ~ closeParen) ^^ {
    case dimension ~ openParen ~ name ~ closeParen =>
      name
  }

  def subunitsArg: Parser[Map[String, ScrubJayUnitsField]] = (subunits ~ openParen ~ name ~ colon ~ unitsMember ~ closeParen) ^^ {
    case dimension ~ openParen ~ name ~ colon ~ unitsMember ~ closeParen =>
      Map(name -> unitsMember)
  }


  //UNITS(name(list), SUBUNITS(listUnits:UNITS(...))
  //DOMAIN(DIM(node), UNITS(name(list), elemType(MULTIPOINT), subunits(key:UNITS(...), key:UNITS(...) )

  def parseQuery: Parser[ScrubJaySchema] = select ~ fields ^^ {
    case select ~ fields => {
      ScrubJaySchema(fields.toArray)
    }
  }

  def queryToSchema(queryString: String): ScrubJaySchema = {
    parse(parseQuery, queryString) match {
      case Success(matched, _) => {
        verifyQuery(matched.fields)
        matched
      }
      case Failure(msg, _) => throw new Exception("Invalid Query.")
      case Error(msg, _) => throw new Exception("Error while parsing.")
    }
  }

  def buildCaseInsensitiveRegex(str: String): Regex = {
    ("""(?i)\Q""" + str + """\E""").r
  }


  def verifyQuery(fields: Seq[ScrubJayField]) = {
    var hasDomain: Boolean = false
    var hasValue: Boolean = false
    for (field <- fields) {
      if (field.domain) {
        hasDomain = true
      } else {
        hasValue = true
      }
    }
    if (hasDomain ^ hasValue) {
      throw new Exception("Must have at least one domain and one value")
    } else if (!(hasDomain || hasValue)) {
      throw new Exception("Missing both domain and value")
    } else if (!hasDomain) {
      throw new Exception("Must have at least one domain")
    } else if (!hasValue) {
      throw new Exception("Must have at least one value")
    }
  }
}

