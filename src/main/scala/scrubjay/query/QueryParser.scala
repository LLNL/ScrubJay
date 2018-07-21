package scrubjay.query

import scrubjay.query.schema.{ScrubJayColumnSchemaQuery, ScrubJaySchemaQuery, ScrubJayUnitsQuery}

import scala.util.parsing.combinator._
import scala.util.matching.Regex
import scala.language.postfixOps
import scala.collection.mutable.ArrayBuffer
/*
TODO Find out which characters are allowed
 */


case class Members(dim: String, units: Option[ScrubJayUnitsQuery])
case class UnitsArg(typeOfArg:String, nameOfArg: Option[String], subunitsMap: Option[Map[String, ScrubJayUnitsQuery]])


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
     fields: VALUE(DIM(flops)), VALUE(DIM(time), UNITS(seconds)), DOMAIN(DIM(time), UNITS(...))
     fieldPattern: DOMAIN(_________________)
     fieldMembers: DIM(time), UNITS(timestamp)
   */

  def fields: Parser[ArrayBuffer[ScrubJayColumnSchemaQuery]] = fieldPattern ~ rep(comma ~ fieldPattern) ^^ {
    case (field ~ rest) => rest.foldLeft(ArrayBuffer(field)) {
      case (field, ("," ~ rest)) => field :+ rest
    }
  }

  def fieldPattern: Parser[ScrubJayColumnSchemaQuery] = (fieldType ~ openParen ~ fieldMembers ~ closeParen) ^^ {
    case fieldType ~ openParen ~ fieldMembers ~ closeParen => {
      var domain: Boolean = false
      if (fieldType.toLowerCase.equals("domain")) {
        domain = true
      }
      ScrubJayColumnSchemaQuery(domain = Some(domain), name = None, dimension = Some(fieldMembers.dim), units = fieldMembers.units)
    }
  }
  def fieldType: Parser[String] = (domain | value)

  def fieldMembers: Parser[Members] = dimensionMember ~ opt(comma) ~ opt(unitsMember) ^^ {
    case dimensionMember ~ comma ~ unitsMember => {
      Members(dimensionMember, unitsMember)
    }

  }

  def dimensionMember: Parser[String] = (dimension ~ openParen ~ name ~ closeParen) ^^ {
    case dimension ~ openParen ~ name ~ closeParen =>
      name
  }

  //Example: UNITS(name(list), elemType(MULTIPOINT), subunits(key:UNITS(...))
  def unitsMember: Parser[ScrubJayUnitsQuery] = (units ~ openParen ~ unitsItems ~ closeParen) ^^ {
    case units ~ openParen ~ unitsItems ~ closeParen =>
      verifyUnits(unitsItems)
      ScrubJayUnitsQuery(
        findUnitsArgString(unitsItems, "name"),
        findUnitsArgString(unitsItems, "elementType"),
        findUnitsArgString(unitsItems, "aggregator"),
        findUnitsArgString(unitsItems, "interpolator"),
        findUnitsArgMap(unitsItems, "subunits")
      )
  }

  def unitsItems: Parser[Seq[UnitsArg]] = unitsArg ~ rep(comma ~ unitsArg) ^^ {
    case (name ~ list) => list.foldLeft(Seq(name)) {
      case (left, ("," ~ right)) => left :+ right
    }
  }

  def unitsArg: Parser[UnitsArg] = (nameArg | elementTypeArg | aggregatorArg | interpolatorArg | subunitsArg)

  def findUnitsArgString(unitsItems: Seq[UnitsArg], target:String): Option[String] = {
    for (arg <- unitsItems) {
      if (arg.typeOfArg.equals(target)) {
        return arg.nameOfArg
      }
    }
    None
  }

  def findUnitsArgMap(unitsItems: Seq[UnitsArg], target:String): Option[Map[String, ScrubJayUnitsQuery]] = {
    for (arg <- unitsItems) {
      if (arg.typeOfArg.equals(target)) {
        return arg.subunitsMap
      }
    }
    None
  }

  /*
  UNITS(name(...), elementType(...), aggregator(...), interpolator(...), subunits(...))
  UNITS can only have up to five arguments and arguments can be optional as long as one is specified
   */
  def verifyUnits(unitsItems: Seq[UnitsArg]) = {
    if (unitsItems.size > 5 || unitsItems.size < 1) {
      throw new Exception("UNITS field must have between one and five arguments")
    }
    val unitsSet = unitsItems.groupBy(_.typeOfArg).map(_._2.head)
    if (unitsSet.size != unitsItems.size) {
      throw new Exception("UNITS field cannot have duplicate arguments")
    }
  }



  //Make overarching unitArg then check for no duplicates
  //

  def nameArg: Parser[UnitsArg] = (unitName ~ openParen ~ name ~ closeParen) ^^ {
    case dimension ~ openParen ~ name ~ closeParen =>
      UnitsArg(typeOfArg = "name", nameOfArg = Some(name), subunitsMap = None)
  }

  def elementTypeArg: Parser[UnitsArg] = (elementType ~ openParen ~ name ~ closeParen) ^^ {
    case dimension ~ openParen ~ name ~ closeParen =>
      UnitsArg(typeOfArg = "elementType", nameOfArg = Some(name), subunitsMap = None)

  }

  def aggregatorArg: Parser[UnitsArg] = (aggregator ~ openParen ~ name ~ closeParen) ^^ {
    case dimension ~ openParen ~ name ~ closeParen =>
      UnitsArg(typeOfArg = "aggregator", nameOfArg = Some(name), subunitsMap = None)

  }

  def interpolatorArg: Parser[UnitsArg] = (interpolator ~ openParen ~ name ~ closeParen) ^^ {
    case dimension ~ openParen ~ name ~ closeParen =>
      UnitsArg(typeOfArg = "interpolator", nameOfArg = Some(name), subunitsMap = None)

  }

  def subunitsArg: Parser[UnitsArg] = (subunits ~ openParen ~ name ~ colon ~ unitsMember ~ closeParen) ^^ {
    case dimension ~ openParen ~ name ~ colon ~ unitsMember ~ closeParen =>
      UnitsArg(typeOfArg = "subunits", nameOfArg = None, subunitsMap = Some(Map(name -> unitsMember)))
  }


  //UNITS(name(list), SUBUNITS(listUnits:UNITS(...))
  //DOMAIN(DIM(node), UNITS(name(list), elemType(MULTIPOINT), subunits(key:UNITS(...), key:UNITS(...) )

  def parseQuery: Parser[ScrubJaySchemaQuery] = select ~ fields ^^ {
    case select ~ fields => {
      ScrubJaySchemaQuery(fields.toSet)
    }
  }

  def queryToSchema(queryString: String): ScrubJaySchemaQuery = {
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


  def verifyQuery(fields: Set[ScrubJayColumnSchemaQuery]) = {
    var hasDomain: Boolean = false
    var hasValue: Boolean = false
    for (field <- fields) {
      if (field.domain.isDefined && field.domain.get) {
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

