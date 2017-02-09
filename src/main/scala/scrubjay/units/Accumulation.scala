package scrubjay.units

import scrubjay.metabase.MetaDescriptor._
import scrubjay.units.ConversionHelpers._
import scrubjay.units.UnitsTag.DomainType
import scrubjay.units.UnitsTag.DomainType.DomainType

case class Accumulation(value: Long) extends Units[Long] with Continuous {
  override def asDouble: Double = value.toDouble
}

object Accumulation extends UnitsTag[Accumulation, Long] {

  override val domainType: DomainType = DomainType.QUANTITY

  override def convert(value: Any, metaUnits: MetaUnits): Accumulation = Accumulation(value)

  override protected def createTypedInterpolator(xs: Seq[Double], ys: Seq[Accumulation]): (Double) => Accumulation = {

    val xValues = xs
    val yValues = ys.map(_.value.toDouble)

    val xyValues = xValues.zip(yValues).sortWith(_._1 < _._1)

    val xyDeltas = xyValues.sliding(2).map{case Seq(a,b) => (a,b)}.map{case ((x1,y1), (x2,y2)) => (x2-x1, y2-y1)}

    val xyPosDeltas = xyDeltas.filter(_._2 >= 0)
    val xyDeltaSums = xyPosDeltas.reduce{case ((dx1, dy1), (dx2, dy2)) => (dx1+dx2, dy1+dy2)}

    (d: Double) => Accumulation(xyDeltaSums._2 / xyDeltaSums._1)
  }

  override protected def typedReduce(ys: Seq[Accumulation]): Accumulation = {
    ys.last
  }
}
