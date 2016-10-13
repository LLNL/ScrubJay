package scrubjay.metabase

import scrubjay.metabase.GlobalMetaBase._
import scrubjay.metabase.MetaDescriptor._

case class MetaEntry(meaning: MetaMeaning,
                     dimension: MetaDimension,
                     units: MetaUnits) extends Serializable

object MetaEntry {

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

  def metaEntryFromStrings(meaningString: String,
                           dimensionString: String,
                           unitsString: String): MetaEntry = {
    MetaEntry(
      meaning = META_BASE.meaningBase.getOrElse(meaningString, MEANING_UNKNOWN),
      dimension = META_BASE.dimensionBase.getOrElse(dimensionString, DIMENSION_UNKNOWN),
      units = metaUnitsFromString(unitsString)
    )
  }
}


