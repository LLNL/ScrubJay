package scrubjay.units

import scrubjay.meta._
import scrubjay.units.ConversionHelpers._

import scala.reflect._

case class Count(v: Int) extends Units(v)

object Count extends UnitsTag[Count] {
  override val rawValueClass = classTag[Int]
  override def convert(value: Any, metaUnits: MetaUnits): Count = Count(value)
}

