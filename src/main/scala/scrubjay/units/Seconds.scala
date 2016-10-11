package scrubjay.units

import scala.reflect._
import scrubjay.meta.MetaDescriptor._
import scrubjay.units.ConversionHelpers._
import scrubjay.units.UnitsTag.DomainType
import scrubjay.units.UnitsTag.DomainType.DomainType

case class Seconds(value: Double) extends Units[Double]

object Seconds extends UnitsTag[Seconds] {
  override val rawValueClassTag = classTag[Double]
  override val domainType: DomainType = DomainType.POINT
  override def convert(value: Any, metaUnits: MetaUnits): Seconds = Seconds(value)
}
