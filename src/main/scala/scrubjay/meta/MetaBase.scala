package scrubjay.meta

import scrubjay.units._

import scala.reflect._
import scala.reflect.ClassTag

object MetaBase {

  implicit def stringToMeaning(s: String): MetaMeaning = {
    MEANING_BASE.getOrElse(s, MEANING_UNKNOWN)
  }

  implicit def stringToDimension(s: String): MetaDimension = {
    DIMENSION_BASE.getOrElse(s, MetaDimension(s, "composite", compositeClassTag(s)))
  }

  implicit def stringToUnits(s: String): MetaUnits = {
    UNITS_BASE.getOrElse(s, MetaUnits(s, "composite", compositeClassTag(s)))
  }

  def compositeClassTag(s: String): ClassTag[_] = {
    val compositePattern = """(.*)<(.*)>""".r
    if (s.matches(compositePattern.toString)) {
      val compositePattern(composite, prime) = s
      composite match {
        case "list" =>
      }
    }
  }

  // TODO: Change meta bases to final val
  var MEANING_BASE: Map[String, MetaMeaning] = Map.empty
  var DIMENSION_BASE: Map[String, MetaDimension] = Map.empty
  var UNITS_BASE: Map[String, MetaUnits] = Map.empty

  def addToMetaBase[T <: MetaDescriptor](desc: T): T = desc match {
    case m: MetaMeaning => MEANING_BASE ++= Map(m.title -> m); m;
    case d: MetaDimension => DIMENSION_BASE ++= Map(d.title -> d); d;
    case u: MetaUnits => UNITS_BASE ++= Map(u.title -> u); u;
  }

  final val MEANING_UNKNOWN = addToMetaBase(MetaMeaning("unknown", "Life"))
  final val MEANING_START = addToMetaBase(MetaMeaning("start", "The beginning of something"))
  final val MEANING_DURATION = addToMetaBase(MetaMeaning("duration", "A span of time"))

  final val DIMENSION_UNKNOWN = addToMetaBase(MetaDimension("unknown", "The upside down", classTag[NoDimension]))
  final val DIMENSION_TIME = addToMetaBase(MetaDimension("time", "The time dimension", classTag[Time]))

  final val UNITS_IDENTIFIER = addToMetaBase(MetaUnits("identifier", "A categorical identifier", classTag[Identifier]))
  final val UNITS_SECONDS = addToMetaBase(MetaUnits("seconds", "Quantity of seconds", classTag[Seconds]))
}
