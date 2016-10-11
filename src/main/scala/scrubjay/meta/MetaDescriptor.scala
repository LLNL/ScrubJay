package scrubjay.meta

import scrubjay.units._

import scala.language.existentials

sealed abstract class MetaDescriptor extends Serializable {
  val title: String
  val description: String
  override def toString: String = title
}

object MetaDescriptor {

  // Types of MetaDescriptor

  case class MetaMeaning(title: String, description: String) extends MetaDescriptor

  case class MetaDimension(title: String, description: String) extends MetaDescriptor

  case class MetaUnits(title: String, description: String,
                       unitsTag: UnitsTag[_ <: Units[_]],
                       unitsChildren: List[MetaUnits] = List.empty) extends MetaDescriptor {
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
