package scrubjay.units

import scrubjay.meta.MetaDescriptor
import scrubjay.units.ConversionHelpers._

case class Seconds(v: Double) extends Units[Seconds]

object Seconds {

  // Implement converter
  val converter = new UnitsConverter[Seconds] {
    override def convert(value: Any, metaUnits: MetaDescriptor): Seconds = Seconds(value)
  }

}
