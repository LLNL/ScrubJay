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

    val yValues = ys.map(_.value.toDouble)
    val xValues = xs

    val yDeltas = yValues.sliding(2).map(_.reduce((a,b) => b - a))
    val xDeltas = xValues.sliding(2).map(_.reduce((a,b) => b - a))

    val xyDeltas = xDeltas.zip(yDeltas).filter(_._2 > 0)

    val deltaX = xyDeltas.map(_._1).sum
    val deltaY = xyDeltas.map(_._2).sum

    (d: Double) => Accumulation(deltaY / deltaX)
  }

  override protected def typedReduce(ys: Seq[Accumulation]): Accumulation = {
    ys.last
  }
}
