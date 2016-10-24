package scrubjay.units

import breeze.interpolation.LinearInterpolator
import breeze.linalg.DenseVector

import scrubjay.metabase.MetaDescriptor._
import scrubjay.units.ConversionHelpers._
import scrubjay.units.UnitsTag.DomainType
import scrubjay.units.UnitsTag.DomainType.DomainType

case class Seconds(value: Double) extends Units[Double]

object Seconds extends UnitsTag[Seconds, Double] {

  override val domainType: DomainType = DomainType.POINT

  override def convert(value: Any, metaUnits: MetaUnits): Seconds = Seconds(value)

  protected override def createInterpolator(xs: Seq[Double], ys: Seq[Seconds]): (Double) => Seconds = {
    val f = LinearInterpolator(DenseVector(xs:_*), DenseVector(ys.map(_.value):_*))
    (d: Double) => Seconds(f(d))
  }
}
