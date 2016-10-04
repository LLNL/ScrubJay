package scrubjay.units

import scrubjay.meta._

import scala.reflect._
import scala.reflect.ClassTag

abstract class UnitsTag[T <: Units[_]: ClassTag] {
  // FIXME: Infer the rawValueClassTag
  val rawValueClassTag: ClassTag[_]
  def convert(value: Any, metaUnits: MetaUnits): T
}
