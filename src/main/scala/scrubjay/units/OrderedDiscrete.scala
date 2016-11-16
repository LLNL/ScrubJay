package scrubjay.units

import breeze.interpolation.LinearInterpolator
import breeze.linalg.DenseVector
import scrubjay.metabase.MetaDescriptor._
import scrubjay.units.ConversionHelpers._
import scrubjay.units.UnitsTag.DomainType
import scrubjay.units.UnitsTag.DomainType.DomainType

case class OrderedDiscrete(value: Long) extends Units[Long] with Continuous {
  override def asDouble: Double = value.toDouble
}

object OrderedDiscrete extends UnitsTag[OrderedDiscrete, Long] {

  override val domainType: DomainType = DomainType.QUANTITY

  override def convert(value: Any, metaUnits: MetaUnits): OrderedDiscrete = OrderedDiscrete(value)

  override protected def createTypedInterpolator(xs: Seq[Double], ys: Seq[OrderedDiscrete]): (Double) => OrderedDiscrete = {
    val f = LinearInterpolator(DenseVector(xs:_*), DenseVector(ys.map(_.value.toDouble):_*))
    (d: Double) => OrderedDiscrete(Math.round(f(d)))
  }

  override protected def typedReduce(ys: Seq[OrderedDiscrete]): OrderedDiscrete = {
    OrderedDiscrete(Math.round(ys.map(_.value).sum.toDouble / ys.length))
  }
}

