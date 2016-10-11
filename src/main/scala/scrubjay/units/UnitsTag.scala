package scrubjay.units

import scrubjay.meta.MetaDescriptor._

import scala.reflect.ClassTag

abstract class UnitsTag[T <: Units[_]] {
  // FIXME: Infer the rawValueClassTag
  val rawValueClassTag: ClassTag[_]
  def convert(value: Any, metaUnits: MetaUnits): T
}
