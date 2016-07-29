package scrubjay.meta

import MetaBase._

import scala.reflect._
import scala.reflect.ClassTag

import scala.language.existentials

abstract class MetaDescriptor
(val title: String,
 val description: String,
 val tag: ClassTag[_] = classTag[None.type],
 val children: List[MetaDescriptor] = List.empty) extends Serializable {
  override def toString: String = title + { if (children.nonEmpty) "<" + children.map(_.title).mkString(",") + ">" else ""}
}

case class MetaMeaning(t: String, d: String) extends MetaDescriptor(t, d)
case class MetaDimension(t: String, d: String, ct: ClassTag[_], c: List[MetaDescriptor] = List.empty) extends MetaDescriptor(t, d, ct, c)
case class MetaUnits(t: String, d: String, ct: ClassTag[_], c: List[MetaDescriptor] = List.empty) extends MetaDescriptor(t, d, ct, c)

case class MetaEntry(meaning: MetaMeaning,
                     dimension: MetaDimension,
                     units: MetaUnits) extends Serializable

object MetaEntry {
  def metaEntryFromStringTuple(meaning: String, dimension: String, units: String): MetaEntry = {
    MetaEntry(meaning, dimension, units)
  }
}


