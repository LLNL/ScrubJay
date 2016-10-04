package scrubjay.units

import scala.reflect._

import scrubjay.meta._
import scrubjay.units.ConversionHelpers._

case class Seconds(value: Double) extends Units[Double]

object Seconds extends UnitsTag[Seconds] {
  override val rawValueClassTag = classTag[Double]
  override def convert(value: Any, metaUnits: MetaUnits): Seconds = Seconds(value)
}
