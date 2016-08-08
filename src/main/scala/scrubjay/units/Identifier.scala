package scrubjay.units

import scrubjay.meta._

case class Identifier(v: String) extends Units

object Identifier {

  // Implement converter
  val converter = new UnitsConverter[Identifier] {
    override def convert(value: Any, metaUnits: MetaDescriptor): Identifier = Identifier(value.toString)
  }

}
