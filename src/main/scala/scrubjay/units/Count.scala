package scrubjay.units

import scrubjay.meta._
import scrubjay.units.ConversionHelpers._

case class Count(v: Int) extends Units(v)

object Count {

  // Implement converter
  val converter = new UnitsConverter[Count] {
    override def convert(value: Any, metaUnits: MetaUnits): Count = Count(value)
  }

}

