package scrubjay.units

import breeze.interpolation.LinearInterpolator
import breeze.linalg.DenseVector
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
    val f = LinearInterpolator(DenseVector(xs:_*), DenseVector(ys.map(_.value.toDouble):_*))
    (d: Double) => Accumulation(Math.round(f(d)))
  }

  override protected def typedReduce(ys: Seq[Accumulation]): Accumulation = {
    Accumulation(ys.map(_.value).max - ys.map(_.value).min)
  }
}
