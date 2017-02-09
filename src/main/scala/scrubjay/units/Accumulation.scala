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

    case class SimpleVec(x: Double, y: Double) {
      def +(simpleVec: SimpleVec): SimpleVec = SimpleVec(x + simpleVec.x, y+simpleVec.y)
      def -(simpleVec: SimpleVec): SimpleVec = SimpleVec(x - simpleVec.x, y-simpleVec.y)
      def rate: Double = y / x
    }

    val xValues = xs
    val yValues = ys.map(_.value.toDouble)

    val xyValues = xValues.zip(yValues).sortWith(_._1 < _._1).map(a => SimpleVec(a._1, a._2))

    val xyDeltas = xyValues.sliding(2).map{case Seq(a,b) => (a,b)}.map{case (a: SimpleVec, b: SimpleVec) => b - a}

    val xyPosDeltas = xyDeltas.filter(_.y >= 0)
    val xyDeltaSums = xyPosDeltas.reduce(_ + _)

    (d: Double) => Accumulation(xyDeltaSums.rate)
  }

  override protected def typedReduce(ys: Seq[Accumulation]): Accumulation = {
    ys.last
  }
}
