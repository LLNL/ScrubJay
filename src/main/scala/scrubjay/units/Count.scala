package scrubjay.units

import scrubjay.meta.MetaDescriptor
import scrubjay.units.ConversionHelpers._

case class Count(v: Int) extends Units {
  val raw = v
}

object Count {

  // Implement converter
  val converter = new UnitsConverter[Count] {
    override def convert(value: Any, metaUnits: MetaDescriptor): Count = Count(value)
  }

}

