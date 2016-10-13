package scrubjay.units

import scrubjay.metabase.MetaDescriptor._

import scala.reflect.ClassTag

object UnitsTag {

  object DomainType extends Enumeration {
    type DomainType = Value
    val POINT, MULTIPOINT, RANGE, UNKNOWN= Value
  }

}

abstract class UnitsTag[T <: Units[_]] {

  // FIXME: Infer the rawValueClassTag
  val rawValueClassTag: ClassTag[_]

  val domainType: UnitsTag.DomainType.DomainType

  def convert(value: Any, metaUnits: MetaUnits): T
}

