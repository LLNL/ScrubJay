package scrubjay.units

import scrubjay.meta.MetaDescriptor._
import scrubjay.units.ConversionHelpers._

import scrubjay.units.UnitsTag.DomainType
import scrubjay.units.UnitsTag.DomainType.DomainType

import scala.reflect._

case class Count(value: Int) extends Units[Int]

object Count extends UnitsTag[Count] {
  override val rawValueClassTag = classTag[Int]
  override val domainType: DomainType = DomainType.POINT
  override def convert(value: Any, metaUnits: MetaUnits): Count = Count(value)
}

