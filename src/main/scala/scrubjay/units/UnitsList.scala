package scrubjay.units

import scrubjay.meta._
import scrubjay.units.Units._

case class UnitsList[T](v: List[T]) extends Units(v)

object UnitsList {

  // Implement converter
  val converter = new UnitsConverter[UnitsList[_]] {
    override def convert(value: Any, metaUnits: MetaDescriptor): UnitsList[_] = value match {
      case l: List[Any] => UnitsList(l.map(raw2Units(_, metaUnits.children.head)))
      case s: String => UnitsList(s.split(",").map(raw2Units(_, metaUnits.children.head)).toList)
      case v => throw new RuntimeException(s"No known converter from $v to UnitsList")
    }
  }

}
