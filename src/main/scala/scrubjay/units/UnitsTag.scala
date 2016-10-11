package scrubjay.units

import scrubjay.meta.MetaDescriptor._

import scala.reflect.ClassTag

object UnitsTag {

  object DomainType extends Enumeration {
    type DomainType = Value
    val POINT, MULTIPOINT, RANGE = Value
  }

}

abstract class UnitsTag[T <: Units[_]] {

  // FIXME: Infer the rawValueClassTag
  val rawValueClassTag: ClassTag[_]

  val domainType: UnitsTag.DomainType.DomainType

  def convert(value: Any, metaUnits: MetaUnits): T
}

