package scrubjay.meta

import MetaBase._

import scala.reflect._
import scala.reflect.ClassTag


abstract class MetaDescriptor extends Serializable

case class MetaMeaning(title: String, description: String) extends MetaDescriptor
case class MetaDimension(title: String, description: String, tag: ClassTag[_]) extends MetaDescriptor
case class MetaUnits(title: String, description: String, tag: ClassTag[_]) extends MetaDescriptor

case class MetaEntry(meaning: MetaMeaning, 
                     dimension: MetaDimension, 
                     units: MetaUnits) extends Serializable

object MetaEntry {
  def metaEntryFromStringTuple(meaning: String, dimension: String, units: String): MetaEntry = {
    MetaEntry(meaning, dimension, units)
  }
}


