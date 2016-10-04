package scrubjay.meta

import scrubjay.meta.MetaBase._
import scrubjay.units.{Units, UnitsTag}

import scala.language.existentials

abstract class MetaDescriptor extends Serializable {

  val title: String
  val description: String

  override def toString: String = title
}

case class MetaMeaning(override val title: String, override val description: String) extends MetaDescriptor
case class MetaDimension(override val title: String, override val description: String) extends MetaDescriptor
case class MetaUnits(override val title: String,
                     override val description: String,
                     unitsTag: UnitsTag[_ <: Units[_]],
                     unitsChildren: List[MetaUnits] = List.empty) extends MetaDescriptor {

  override def toString: String = super.toString + { if (unitsChildren.nonEmpty) "<" + unitsChildren.map(_.title).mkString(",") + ">" else ""}
}

case class MetaEntry(meaning: MetaMeaning,
                     dimension: MetaDimension,
                     units: MetaUnits) extends Serializable

object MetaEntry {
  def fromStringTuple(meaning: String, dimension: String, units: String): MetaEntry = {
    MetaEntry(meaning, dimension, units)
  }
}


