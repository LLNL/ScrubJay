package scrubjay.units

import scrubjay.meta._
import scrubjay.units.Units._

import scala.reflect._

case class UnitsList[T](v: List[T]) extends Units(v)

object UnitsList extends UnitsTag[UnitsList[_]]{
  override val rawValueClass = classTag[List[_]]
  override def convert(value: Any, metaUnits: MetaUnits): UnitsList[_] = value match {
    case l: List[Any] => UnitsList(l.map(raw2Units(_, metaUnits.unitsChildren.head)))
    case s: String => UnitsList(s.split(",").map(raw2Units(_, metaUnits.unitsChildren.head)).toList)
    case v => throw new RuntimeException(s"Cannot convert $v to $metaUnits")
  }
}
