package scrubjay.units

import scrubjay.meta.MetaDescriptor._
import scrubjay.units.UnitsTag.DomainType
import scrubjay.units.UnitsTag.DomainType.DomainType

import scala.reflect._

case class Identifier(value: String) extends Units[String]

object Identifier extends UnitsTag[Identifier] {
  override val rawValueClassTag = classTag[String]
  override val domainType: DomainType = DomainType.POINT
  override def convert(value: Any, metaUnits: MetaUnits): Identifier = Identifier(value.toString)
}
