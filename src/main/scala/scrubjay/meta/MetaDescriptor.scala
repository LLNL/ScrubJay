package scrubjay.meta

import scrubjay.units._

import scala.language.existentials


object MetaDescriptor {

  object DimensionType extends Enumeration {
    type DimensionType = Value
    val CONTINUOUS, DISCRETE = Value
  }

  case class MetaMeaning(title: String, description: String)

  case class MetaDimension(title: String, description: String, dimensionType: DimensionType.DimensionType)

  case class MetaUnits(title: String, description: String,
                       unitsTag: UnitsTag[_ <: Units[_]],
                       unitsChildren: List[MetaUnits] = List.empty) {
    override def toString: String = {
      super.toString + {
        if (unitsChildren.nonEmpty)
          "<" + unitsChildren.map(_.title).mkString(",") + ">"
        else
          ""
      }
    }
  }
}
