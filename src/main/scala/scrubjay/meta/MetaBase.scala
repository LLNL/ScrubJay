package scrubjay.meta

import scrubjay.units._

import scala.language.implicitConversions
import scala.reflect._
import scala.reflect.ClassTag

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
      META_BASE.unitsBase.getOrElse(s, UNITS_IDENTIFIER)
  }

  implicit val stringToTaggedConverter = new StringToMetaConverter[TaggedMeta] {
    override def convert(s: String): TaggedMeta =
      //TODO: No nulls!
      collectionClassTag[MetaUnits](s).orNull
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

  implicit def stringToTagged(s: String): TaggedMeta = {
    implicitly[StringToMetaConverter[TaggedMeta]].convert(s)
  }

  def collectionClassTag[M <: TaggedMeta](s: String)(implicit converter: StringToMetaConverter[M]): Option[MetaCollection] = {
    val compositePattern = """(.*)<(.*)>""".r
    if (s.matches(compositePattern.toString)) {
      val compositePattern(composite, childrenToken) = s
      val childrenTokens = childrenToken.split(",")
      val children = childrenTokens.map(converter.convert)
      composite match {
        //TODO: Decouple collections from here
        case "list" => Some(MetaCollection("list", "A list of...", classTag[List[_]], children:_*))
        case "rate" => Some(MetaCollection("rate", "A rate of...", classTag[(_,_)], children:_*))
      }
    }
    else {
      None
    }
  }

}
