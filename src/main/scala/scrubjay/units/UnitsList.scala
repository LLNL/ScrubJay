package scrubjay.units

import scrubjay.meta._
import scrubjay.units.Units._

case class UnitsList[T](v: List[T]) extends Units[UnitsList[T]]

object UnitsList {

  // Implement converter
  val converter = new UnitsConverter[UnitsList[_]] {
    override def convert(value: Any, metaUnits: MetaDescriptor): UnitsList[_] = value match {
      case l: List[Any] => UnitsList(l.map(iv1 => raw2Units(iv1, metaUnits.children.head)))
    }
  }

}
