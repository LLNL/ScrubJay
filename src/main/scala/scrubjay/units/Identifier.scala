package scrubjay.units

import scrubjay.meta._

import scala.reflect._

case class Identifier(v: String) extends Units(v)

object Identifier extends UnitsTag[Identifier] {
  override val rawValueClass = classTag[String]
  override def convert(value: Any, metaUnits: MetaUnits): Identifier = Identifier(value.toString)
}
