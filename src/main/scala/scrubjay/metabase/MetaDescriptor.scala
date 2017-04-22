package scrubjay.metabase

import scala.language.existentials


object MetaDescriptor {

  case class MetaDimension(title: String, description: String, dimensionType: DimensionSpace.DimensionSpace)

  case class MetaUnits(title: String, description: String,
                       //unitsTag: UnitsTag[_ <: Units[_], _],
                       unitsChildren: List[MetaUnits] = List.empty) {

    /*
    override def equals(obj: scala.Any): Boolean = {
      obj match {
        case MetaUnits(objTitle, objDescription, objUnitsTag, _) =>
          title == objTitle && description == objDescription && unitsTag == objUnitsTag
      }
    }
    */

    override def toString: String = {
      title + {
        if (unitsChildren.nonEmpty)
          "<" + unitsChildren.map(_.title).mkString(",") + ">"
        else
          ""
      }
    }
  }

  object DimensionSpace extends Enumeration {
    type DimensionSpace = Value
    val CONTINUOUS, DISCRETE = Value
  }

}
