package scrubjay.meta

import scrubjay.meta.GlobalMetaBase._

import scala.language.implicitConversions

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

  // Recursive meta units parser
  def metaUnitsFromString(s: String): MetaUnits = {
    META_BASE.unitsBase.getOrElse(s, {
      val compositePattern = """(.*)<(.*)>""".r
      if (s.matches(compositePattern.toString)) {
        val compositePattern(composite, childrenToken) = s
        val childrenTokens = childrenToken.split(",")
        val compositeChildren = childrenTokens.map(metaUnitsFromString).toList
        metaUnitsFromString(composite).copy(unitsChildren = compositeChildren)
      }
      else {
        UNITS_UNKNOWN
      }
    })
  }

  // Type class for implicit conversions
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

  // Implicit conversions
  implicit def stringToMeaning(s: String): MetaMeaning = {
    implicitly[StringToMetaConverter[MetaMeaning]].convert(s)
  }

  implicit def stringToDimension(s: String): MetaDimension = {
    implicitly[StringToMetaConverter[MetaDimension]].convert(s)
  }

  implicit def stringToUnits(s: String): MetaUnits = {
    implicitly[StringToMetaConverter[MetaUnits]].convert(s)
  }

}
