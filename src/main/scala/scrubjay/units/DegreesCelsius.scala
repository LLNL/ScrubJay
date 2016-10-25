package scrubjay.units

import scrubjay.metabase.MetaDescriptor.MetaUnits
import scrubjay.units.UnitsTag.DomainType
import scrubjay.units.UnitsTag.DomainType.DomainType
import scrubjay.units.ConversionHelpers._

import breeze.interpolation.LinearInterpolator
import breeze.linalg.DenseVector

case class DegreesCelsius(value: Double) extends Units[Double]

object DegreesCelsius extends UnitsTag[DegreesCelsius, Double] {

  override val domainType: DomainType = DomainType.QUANTITY

  override def convert(value: Any, metaUnits: MetaUnits): DegreesCelsius = DegreesCelsius(value)

  override protected def createTypedInterpolator(xs: Seq[Double], ys: Seq[DegreesCelsius]): (Double) => DegreesCelsius = {
    val f = LinearInterpolator(DenseVector(xs:_*), DenseVector(ys.map(_.value):_*))
    (d: Double) => DegreesCelsius(f(d))
  }

  override protected def typedReduce(ys: Seq[DegreesCelsius]): DegreesCelsius = {
    DegreesCelsius(ys.foldLeft(0.0)(_ + _.value) / ys.length)
  }
}
