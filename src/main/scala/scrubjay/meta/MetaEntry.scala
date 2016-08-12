package scrubjay.meta

import scrubjay.meta.MetaBase._

import scala.language.existentials
import scala.reflect.{ClassTag, _}

abstract class MetaDescriptor
(val title: String,
 val description: String,
 val tag: ClassTag[_] = classTag[None.type],
 val children: List[MetaDescriptor] = List.empty) extends Serializable {
  override def toString: String = title + { if (children.nonEmpty) "<" + children.map(_.title).mkString(",") + ">" else ""}
}

case class MetaMeaning(t: String, d: String) extends MetaDescriptor(t, d)
case class MetaDimension(t: String, d: String, ct: ClassTag[_] = classTag[Any], c: List[MetaDescriptor] = List.empty) extends MetaDescriptor(t, d, ct, c)
case class MetaUnits(t: String, d: String, ct: ClassTag[_], c: List[MetaDescriptor] = List.empty) extends MetaDescriptor(t, d, ct, c)

case class MetaEntry(meaning: MetaMeaning,
                     dimension: MetaDimension,
                     units: MetaUnits) extends Serializable

object MetaEntry {
  def fromStringTuple(meaning: String, dimension: String, units: String): MetaEntry = {
    MetaEntry(meaning, dimension, units)
  }
}


