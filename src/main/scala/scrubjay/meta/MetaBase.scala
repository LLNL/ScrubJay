package scrubjay.meta

import scrubjay.units._

import scala.language.implicitConversions
import scala.reflect._

class MetaBase(mb: Map[String, MetaMeaning] = Map.empty,
               db: Map[String, MetaDimension] = Map.empty,
               ub: Map[String, MetaUnits] = Map.empty) extends Serializable {

  var meaningBase: Map[String, MetaMeaning] = mb
  var dimensionBase: Map[String, MetaDimension] = db
  var unitsBase: Map[String, MetaUnits] = ub

  def addMeaning(m: MetaMeaning): MetaMeaning = { meaningBase ++= Map(m.title -> m); m; }
  def addDimension(m: MetaDimension): MetaDimension = { dimensionBase ++= Map(m.title -> m); m; }
  def addUnits(m: MetaUnits): MetaUnits = { unitsBase ++= Map(m.title -> m); m; }
}

object MetaBase {

  final val META_BASE: MetaBase = new MetaBase

  final val MEANING_UNKNOWN = META_BASE.addMeaning(MetaMeaning("unknown", "Life"))
  final val MEANING_IDENTITY = META_BASE.addMeaning(MetaMeaning("identity", "A single identity"))
  final val MEANING_START = META_BASE.addMeaning(MetaMeaning("start", "The beginning of something"))
  final val MEANING_DURATION = META_BASE.addMeaning(MetaMeaning("duration", "A span of time"))
  final val MEANING_NODE = META_BASE.addMeaning(MetaMeaning("node", "A single node in an HPC cluster"))
  final val MEANING_RACK = META_BASE.addMeaning(MetaMeaning("rack", "A rack (containing nodes) in an HPC cluster"))

  final val DIMENSION_UNKNOWN = META_BASE.addDimension(MetaDimension("unknown", "The upside down", classTag[NoDimension]))
  final val DIMENSION_TIME = META_BASE.addDimension(MetaDimension("time", "The time dimension", classTag[Time]))

  final val UNITS_IDENTIFIER: MetaUnits = META_BASE.addUnits(MetaUnits("identifier", "A categorical identifier", classTag[Identifier]))
  final val UNITS_SECONDS = META_BASE.addUnits(MetaUnits("seconds", "Quantity of seconds", classTag[Seconds]))

  trait StringToMetaConverter[M <: MetaDescriptor] {
    def convert(s: String): M
  }

  implicit val stringToMetaMeaningConverter = new StringToMetaConverter[MetaMeaning] {
    override def convert(s: String): MetaMeaning = META_BASE.meaningBase.getOrElse(s, MEANING_UNKNOWN)
  }

  implicit val stringToMetaDimensionConverter = new StringToMetaConverter[MetaDimension] {
    override def convert(s: String): MetaDimension =
      META_BASE.dimensionBase.getOrElse(s, DIMENSION_UNKNOWN)
  }

  implicit val stringToMetaUnitConverter = new StringToMetaConverter[MetaUnits] {
    override def convert(s: String): MetaUnits =
      metaUnitsFromString(s)
  }

  implicit def stringToMeaning(s: String): MetaMeaning = {
    implicitly[StringToMetaConverter[MetaMeaning]].convert(s)
  }

  implicit def stringToDimension(s: String): MetaDimension = {
    implicitly[StringToMetaConverter[MetaDimension]].convert(s)
  }

  implicit def stringToUnits(s: String): MetaUnits = {
    implicitly[StringToMetaConverter[MetaUnits]].convert(s)
  }

  def metaUnitsFromString(s: String): MetaUnits = {
    META_BASE.unitsBase.getOrElse(s, {
      val compositePattern = """(.*)<(.*)>""".r
      if (s.matches(compositePattern.toString)) {
        val compositePattern(composite, childrenToken) = s
        val childrenTokens = childrenToken.split(",")
        val children = childrenTokens.map(metaUnitsFromString).toList
        composite match {
          //TODO: Decouple collections from here
          case "list" => MetaUnits("list", "A list of...", classTag[UnitList[_]], children)
          case "rate" => MetaUnits("rate", "A rate of...", classTag[(_, _)], children)
          case _ => UNITS_IDENTIFIER
        }
      }
      else {
        UNITS_IDENTIFIER
      }
    })
  }

}
