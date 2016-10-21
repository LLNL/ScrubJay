package scrubjay.units

import breeze.interpolation.LinearInterpolator
import breeze.linalg.DenseVector
import scrubjay.metabase.MetaDescriptor._
import scrubjay.units.ConversionHelpers._
import scrubjay.units.UnitsTag.DomainType
import scrubjay.units.UnitsTag.DomainType.DomainType

import scala.reflect._

case class Count(value: Long) extends Units[Long] with Continuous {
  override def asDouble: Double = value.toDouble
}

object Count extends UnitsTag[Count, Long] {

  override val domainType: DomainType = DomainType.POINT

  override def convert(value: Any, metaUnits: MetaUnits): Count = Count(value)
  override def createInterpolator(xs: Seq[Double], ys: Seq[Count]): (Double) => Count = {
    val f = LinearInterpolator(DenseVector(xs:_*), DenseVector(ys.map(_.value.toDouble):_*))
    (d: Double) => Count(Math.round(f(d)))
  }
}

