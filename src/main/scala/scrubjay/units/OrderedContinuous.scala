package scrubjay.units

import breeze.interpolation.LinearInterpolator
import breeze.linalg.DenseVector
import scrubjay.metabase.MetaDescriptor._
import scrubjay.units.ConversionHelpers._
import scrubjay.units.UnitsTag.DomainType
import scrubjay.units.UnitsTag.DomainType.DomainType

case class OrderedContinuous(value: Double) extends Units[Double] with Continuous {
  override def asDouble: Double = value
}

object OrderedContinuous extends UnitsTag[OrderedContinuous, Double] {

  override val domainType: DomainType = DomainType.QUANTITY

  override def convert(value: Any, metaUnits: MetaUnits): OrderedContinuous = OrderedContinuous(value)

  override protected def createTypedInterpolator(xs: Seq[Double], ys: Seq[OrderedContinuous]): (Double) => OrderedContinuous = {
    val f = LinearInterpolator(DenseVector(xs:_*), DenseVector(ys.map(_.value):_*))
    (d: Double) => OrderedContinuous(f(d))
  }

  override protected def typedReduce(ys: Seq[OrderedContinuous]): OrderedContinuous = {
    OrderedContinuous(ys.map(_.value).sum / ys.length)
  }
}

