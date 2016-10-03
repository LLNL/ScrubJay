package scrubjay.meta

import scrubjay.meta.MetaBase._

import scala.language.existentials
import scala.reflect.{ClassTag, _}

abstract class MetaDescriptor extends Serializable {

  val title: String
  val description: String
  val unitsChildren: List[MetaUnits] = List.empty

  override def toString: String = title + { if (unitsChildren.nonEmpty) "<" + unitsChildren.map(_.title).mkString(",") + ">" else ""}
}

case class MetaMeaning(override val title: String, override val description: String) extends MetaDescriptor
case class MetaDimension(override val title: String, override val description: String) extends MetaDescriptor
case class MetaUnits(override val title: String,
                     override val description: String,
                     classtag: ClassTag[_] = classTag[Nothing],
                     override val unitsChildren: List[MetaUnits] = List.empty) extends MetaDescriptor

case class MetaEntry(meaning: MetaMeaning,
                     dimension: MetaDimension,
                     units: MetaUnits) extends Serializable

object MetaEntry {
  def fromStringTuple(meaning: String, dimension: String, units: String): MetaEntry = {
    MetaEntry(meaning, dimension, units)
  }
}


