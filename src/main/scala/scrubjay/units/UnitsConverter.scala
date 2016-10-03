package scrubjay.units

import scrubjay.meta._

abstract class UnitsConverter[T] {
  def convert(value: Any, metaUnits: MetaUnits = GlobalMetaBase.UNITS_UNKNOWN): Units[_]
}
