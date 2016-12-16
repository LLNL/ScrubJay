package scrubjay.metabase

import scrubjay.units._

import scala.language.existentials


object MetaDescriptor {

  object DimensionSpace extends Enumeration {
    type DimensionSpace = Value
    val CONTINUOUS, DISCRETE = Value
  }

  object MetaRelationType extends Enumeration {
    type MetaRelationType = Value
    val DOMAIN, VALUE = Value

    def toString(relationType: MetaRelationType): String = relationType match {
      case DOMAIN => "domain"
      case VALUE => "value"
    }

    def fromString(relationTypeString: String): MetaRelationType = relationTypeString match {
      case "domain" => DOMAIN
      case "value"  => VALUE
      case _ => VALUE // Default to VALUE so we don't join on an unknown column
    }
  }

  case class MetaMeaning(title: String, description: String)

  case class MetaDimension(title: String, description: String, dimensionType: DimensionSpace.DimensionSpace)

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
