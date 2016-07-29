package scrubjay.meta

import MetaBase._

import scala.reflect.ClassTag
import scala.language.existentials


abstract class MetaDescriptor(val title: String, val description: String) extends Serializable

case class MetaMeaning(t: String, d: String) extends MetaDescriptor(t, d)

abstract class TaggedMeta(t: String, d: String, val tag: ClassTag[_]) extends MetaDescriptor(t, d)
case class MetaDimension(t: String, d: String, c: ClassTag[_]) extends TaggedMeta(t, d, c)
case class MetaUnits(t: String, d: String, c: ClassTag[_]) extends TaggedMeta(t, d, c)

case class MetaCollection
(t: String, d: String, c: ClassTag[_], children: TaggedMeta*) extends TaggedMeta(t, d, c) {
  override def toString: String = title + "<" + children.map(_.title).mkString(",") + ">"
}

case class MetaEntry(meaning: MetaMeaning,
                     dimension: MetaDimension,
                     units: MetaUnits) extends Serializable

object MetaEntry {
  def metaEntryFromStringTuple(meaning: String, dimension: String, units: String): MetaEntry = {
    MetaEntry(meaning, dimension, units)
  }
}


