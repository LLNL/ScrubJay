package scrubjay.units

import scrubjay.meta._
import scrubjay.units.ConversionHelpers._

import scala.reflect._

case class Count(value: Int) extends Units[Int]

object Count extends UnitsTag[Count] {
  override val rawValueClassTag = classTag[Int]
  override def convert(value: Any, metaUnits: MetaUnits): Count = Count(value)
}

