package scrubjay.units

import scrubjay.meta._

import scala.reflect._

case class Identifier(value: String) extends Units[String]

object Identifier extends UnitsTag[Identifier] {
  override val rawValueClass = classTag[String]
  override def convert(value: Any, metaUnits: MetaUnits): Identifier = Identifier(value.toString)
}
