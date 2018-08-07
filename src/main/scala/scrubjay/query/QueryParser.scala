package scrubjay.query

import scrubjay.query.schema.{ScrubJayColumnSchemaQuery, ScrubJayDimensionSchemaQuery, ScrubJaySchemaQuery, ScrubJayUnitsSchemaQuery}

import scala.util.parsing.combinator._
import scala.util.matching.Regex
import scala.language.postfixOps
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
/*
TODO Find out which characters are allowed
 */


case class Members(dim: Option[ScrubJayDimensionSchemaQuery], units: Option[ScrubJayUnitsSchemaQuery])
case class UnitsArg(name: String, subUnitsName: Option[String] = None, subunitsMap: Option[Map[String, ScrubJayUnitsSchemaQuery]] = None)
case class DimensionArg(name: Option[String] = None, subDimensions: Option[Seq[ScrubJayDimensionSchemaQuery]] = None)

object QueryParser extends RegexParsers {
  /*
  Keywords
    Define specific words that must be matched in the query.
  */
  def select: Parser[String] =  buildCaseInsensitiveRegex("select")
  def domain: Parser[String] = buildCaseInsensitiveRegex("domain")
  def value: Parser[String] = buildCaseInsensitiveRegex("value")
  def units: Parser[String] = buildCaseInsensitiveRegex("units")
  def dimension: Parser[String] = buildCaseInsensitiveRegex("dim")
  def subDimensions: Parser[String] = buildCaseInsensitiveRegex("subdims")

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
    nonQuotedString: Set of characters allowed in a non-quoted name.
    quotedString   : Defines the structure of a quoted name.
    noQuotes     : Rule to define a set of characters allowed in a quoted name (everything except quotes for now)
  */

  def optionallyQuotedString: Parser[String] = (quotedString | nonQuotedString)
  def nonQuotedString: Parser[String] = """[a-zA-Z0-9_*.]+""".r ^^ {_.toString}
  def quotedString: Parser[String] = quote ~ noQuotes ~ quote ^^ {
    case openQuote ~ noQuotes ~ closeQuote => noQuotes.toString
  }
  def noQuotes: Parser[String] = """[^\"]*""".r ^^ {_.toString}

  def name: Parser[String] = buildCaseInsensitiveRegex("name") ~ openParen ~ optionallyQuotedString ~ closeParen ^^ {
    case _ ~ _ ~ name ~ _ => name
  }

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
     columns: VALUE(DIM(flops)), VALUE(DIM(time), UNITS(seconds)), DOMAIN(DIM(time), UNITS(...))
     columnField: DOMAIN(_________________)
     columnMembers: DIM(time), UNITS(timestamp)
   */

  def columns: Parser[ArrayBuffer[ScrubJayColumnSchemaQuery]] = columnField ~ rep(comma ~ columnField) ^^ {
    case (field ~ rest) => rest.foldLeft(ArrayBuffer(field)) {
      case (field, ("," ~ rest)) => field :+ rest
    }
  }

  def columnField: Parser[ScrubJayColumnSchemaQuery] = (columnType ~ openParen ~ columnMembers ~ closeParen) ^^ {
    case fieldType ~ openParen ~ fieldMembers ~ closeParen => {
      var domain: Boolean = false
      if (fieldType.toLowerCase.equals("domain")) {
        domain = true
      }
      ScrubJayColumnSchemaQuery(domain = Some(domain), name = None, dimension = fieldMembers.dim, units = fieldMembers.units)
    }
  }
  def columnType: Parser[String] = (domain | value)

  def columnMembers: Parser[Members] = dimensionField ~ opt(comma) ~ opt(unitsMember) ^^ {
    case dimensionMember ~ comma ~ unitsMember => {
      Members(Some(dimensionMember), unitsMember)
    }
  }

  def dimensionField: Parser[ScrubJayDimensionSchemaQuery] = (dimension ~ openParen ~  dimensionArgList ~ closeParen) ^^ {
    case dimension ~ openParen ~ members ~ closeParen => {

      val dimensionName = members.flatMap(_.name)
      val dimensionSubDimensions = members.flatMap(_.subDimensions)

      // Can only define each argument once
      if (dimensionName.length > 1) {
        throw new Exception("More than one name or defined for dimension!")
      } else if (dimensionSubDimensions.length > 1) {
        throw new Exception("More than one subunits list defined for dimension!")
      }

      val name: Option[String] = dimensionName.headOption
      val subDimensions: Option[Seq[ScrubJayDimensionSchemaQuery]] = dimensionSubDimensions.headOption

      ScrubJayDimensionSchemaQuery(name = name, subDimensions = subDimensions)
    }
  }


  def dimensionSubDimensionListArg: Parser[DimensionArg] = subDimensions ~ openParen ~ dimensionField ~ rep(comma ~ dimensionField) ~ closeParen ^^ {
    // remove parens and identifiers, extract list of dimensions
    case _ ~ _ ~ member ~ rest ~ _ => DimensionArg(subDimensions = Some(member +: rest.map{
      case _ ~ otherMember => otherMember
    }))
  }

  def dimensionArgList: Parser[Seq[DimensionArg]] = dimensionMember ~ rep(comma ~ dimensionMember) ^^ {
    case member ~ rest => member +: rest.map{
      case comma ~ otherMember => otherMember
    }
  }

  def dimensionNameArg: Parser[DimensionArg] = name ^^ {
    case name => DimensionArg(name = Some(name))
  }

  def dimensionMember: Parser[DimensionArg] = (dimensionNameArg | dimensionSubDimensionListArg)

  //Example: UNITS(name(list), elemType(MULTIPOINT), subunits(key:UNITS(...))
  def unitsMember: Parser[ScrubJayUnitsSchemaQuery] = (units ~ openParen ~ unitsItems ~ closeParen) ^^ {
    case units ~ openParen ~ unitsItems ~ closeParen =>
      verifyUnits(unitsItems)
      ScrubJayUnitsSchemaQuery(
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
      if (arg.name.equals(target)) {
        return arg.subUnitsName
      }
    }
    None
  }

  def findUnitsArgMap(unitsItems: Seq[UnitsArg], target:String): Option[Map[String, ScrubJayUnitsSchemaQuery]] = {
    for (arg <- unitsItems) {
      if (arg.name.equals(target)) {
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
    val unitsSet = unitsItems.groupBy(_.name).map(_._2.head)
    if (unitsSet.size != unitsItems.size) {
      throw new Exception("UNITS field cannot have duplicate arguments")
    }
  }



  //Make overarching unitArg then check for no duplicates
  //

  def nameArg: Parser[UnitsArg] = name ^^ {
    case name =>
      UnitsArg(name = "name", subUnitsName = Some(name), subunitsMap = None)
  }

  def elementTypeArg: Parser[UnitsArg] = (elementType ~ openParen ~ optionallyQuotedString ~ closeParen) ^^ {
    case dimension ~ openParen ~ name ~ closeParen =>
      UnitsArg(name = "elementType", subUnitsName = Some(name), subunitsMap = None)

  }

  def aggregatorArg: Parser[UnitsArg] = (aggregator ~ openParen ~ name ~ closeParen) ^^ {
    case dimension ~ openParen ~ name ~ closeParen =>
      UnitsArg(name = "aggregator", subUnitsName = Some(name), subunitsMap = None)

  }

  def interpolatorArg: Parser[UnitsArg] = (interpolator ~ openParen ~ name ~ closeParen) ^^ {
    case dimension ~ openParen ~ name ~ closeParen =>
      UnitsArg(name = "interpolator", subUnitsName = Some(name), subunitsMap = None)

  }

  def subunitsArg: Parser[UnitsArg] = (subunits ~ openParen ~ optionallyQuotedString ~ colon ~ unitsMember ~ closeParen) ^^ {
    case dimension ~ openParen ~ optionallyQuotedString ~ colon ~ unitsMember ~ closeParen =>
      UnitsArg(name = "subunits", subUnitsName = None, subunitsMap = Some(Map(optionallyQuotedString -> unitsMember)))
  }


  //UNITS(name(list), SUBUNITS(listUnits:UNITS(...))
  //DOMAIN(DIM(node), UNITS(name(list), elemType(MULTIPOINT), subunits(key:UNITS(...), key:UNITS(...) )

  def parseQuery: Parser[ScrubJaySchemaQuery] = select ~ columns ^^ {
    case select ~ fields => {
      ScrubJaySchemaQuery(fields.toSet)
    }
  }

  def queryToSchema(queryString: String): ScrubJaySchemaQuery = {
    parse(parseQuery, queryString) match {
      case Success(matched, _) => {
        verifyQuery(matched.columns)
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

