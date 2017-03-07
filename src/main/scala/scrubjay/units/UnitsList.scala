package scrubjay.units

import scrubjay.metabase.MetaDescriptor._
import scrubjay.units.Units._
import scrubjay.units.UnitsTag.DomainType
import scrubjay.units.UnitsTag.DomainType.DomainType

import scala.language.higherKinds

// FIXME: T should be <: Units[_]
case class UnitsList[T](value: List[T]) extends Units[List[T]] with DiscreteRange {
  override def rawString: String = "'" + value.map(_.asInstanceOf[Units[_]].rawString).mkString(",") + "'"
  override def explode: Iterator[Units[_]] = value.asInstanceOf[List[Units[_]]].toIterator
}

object UnitsList extends UnitsTag[UnitsList[_], List[_]] {

  override val domainType: DomainType = DomainType.MULTIPOINT

  def nodeListParse(nl: String): String = {
    // Parse a list formatted like:
    //   "cab[2-6, 15, 19, 22-28]"
    // into:
    //   "cab2, cab3, cab4, cab5, cab6, cab15, cab19, cab22, cab23, ... "
    val nls = nl.substring(0, nl.length-1)
    val nsplit = nls.split("\\[").toSeq

    nsplit match {
      case Seq(a) => a
      case Seq(cluster, lists) => {
        lists.split(",").flatMap(r => r.split("-").toSeq match {
          case Seq(a, b) => a.toInt to b.toInt;
          case b => b
        })
      }
        .map (cluster + _)
        .mkString (",")
    }
  }

  override def convert(value: Any, metaUnits: MetaUnits): UnitsList[_] = value match {
    case l: List[Any] => {
      val unitsList = l.map(raw2Units(_, metaUnits.unitsChildren.head))
      UnitsList(unitsList)
    }
    case s: String => UnitsList(s.split(",").map(raw2Units(_, metaUnits.unitsChildren.head)).toList)
    case v => throw new RuntimeException(s"Cannot convert $v to $metaUnits")
  }

  override protected def createTypedInterpolator(xs: Seq[Double], ys: Seq[UnitsList[_]]): (Double) => UnitsList[_] = {
    (d: Double) => xs.zip(ys).minBy{case (x, _) => Math.abs(x - d)}._2
  }

  override protected def typedReduce(ys: Seq[UnitsList[_]]): UnitsList[_] = {
    ys.head.copy(ys.map(_.value).reduce(_ ++ _))
  }
}
