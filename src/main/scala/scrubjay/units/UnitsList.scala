package scrubjay.units

import scrubjay.meta.MetaDescriptor._
import scrubjay.units.Units._

import scala.reflect._

case class UnitsList[T](value: List[T]) extends Units[List[T]]

object UnitsList extends UnitsTag[UnitsList[_]]{
  override val rawValueClassTag = classTag[List[_]]
  override def convert(value: Any, metaUnits: MetaUnits): UnitsList[_] = value match {
    case l: List[Any] => UnitsList(l.map(raw2Units(_, metaUnits.unitsChildren.head)))
    case s: String => UnitsList(s.split(",").map(raw2Units(_, metaUnits.unitsChildren.head)).toList)
    case v => throw new RuntimeException(s"Cannot convert $v to $metaUnits")
  }
}
