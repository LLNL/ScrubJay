package scrubjay.metabase

import scrubjay.units._

import scala.language.existentials


object MetaDescriptor {

  object DimensionType extends Enumeration {
    type DimensionType = Value
    val CONTINUOUS, DISCRETE, UNKNOWN = Value
  }

  case class MetaMeaning(title: String, description: String)

  case class MetaDimension(title: String, description: String, dimensionType: DimensionType.DimensionType)

  case class MetaUnits(title: String, description: String,
                       unitsTag: UnitsTag[_ <: Units[_], _],
                       unitsChildren: List[MetaUnits] = List.empty) {

    override def equals(obj: scala.Any): Boolean = {
      obj match {
        case MetaUnits(objTitle, objDescription, objUnitsTag, _) =>
          title == objTitle && description == objDescription && unitsTag == objUnitsTag
      }
    }

    override def toString: String = {
      title + {
        if (unitsChildren.nonEmpty)
          "<" + unitsChildren.map(_.title).mkString(",") + ">"
        else
          ""
      }
    }
  }
}
