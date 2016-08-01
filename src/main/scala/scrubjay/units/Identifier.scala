package scrubjay.units

import scrubjay.meta._

case class Identifier(v: Any) extends Units[Identifier]

object Identifier {

  // Implement converter
  val converter = new UnitsConverter[Identifier] {
    override def convert(value: Any, metaUnits: MetaDescriptor): Identifier = Identifier(value)
  }

}
