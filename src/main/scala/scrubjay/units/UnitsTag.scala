package scrubjay.units

import scrubjay.meta._

import scala.reflect.ClassTag

abstract class UnitsTag[T <: Units[_]] {
  val rawValueClass: ClassTag[_]
  def convert(value: Any, metaUnits: MetaUnits): T
}
