package scrubjay.units

import scrubjay.meta._
import scrubjay.units.ConversionHelpers._

case class Seconds(v: Double) extends Units(v)

object Seconds {

  // Implement converter
  val converter = new UnitsConverter[Seconds] {
    override def convert(value: Any, metaUnits: MetaUnits): Seconds = Seconds(value)
  }

}
