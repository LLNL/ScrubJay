package scrubjay.units

import scala.reflect._

import scrubjay.meta._
import scrubjay.units.ConversionHelpers._

case class Seconds(v: Double) extends Units(v)

object Seconds extends UnitsTag[Seconds] {
  override val rawValueClass = classTag[Double]
  override def convert(value: Any, metaUnits: MetaUnits): Seconds = Seconds(value)
}
