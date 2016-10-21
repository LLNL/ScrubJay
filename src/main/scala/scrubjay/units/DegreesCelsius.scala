package scrubjay.units

import breeze.interpolation.LinearInterpolator
import breeze.linalg.DenseVector
import scrubjay.metabase.MetaDescriptor.MetaUnits
import scrubjay.units.UnitsTag.DomainType
import scrubjay.units.UnitsTag.DomainType.DomainType
import scrubjay.units.ConversionHelpers._

import scala.reflect._

case class DegreesCelsius(value: Double) extends Units[Double]

object DegreesCelsius extends UnitsTag[DegreesCelsius] {

  override val rawValueClassTag = classTag[Double]
  override val domainType: DomainType = DomainType.POINT

  override def convert(value: Any, metaUnits: MetaUnits): DegreesCelsius = DegreesCelsius(value)

  override def createInterpolator(xs: Seq[Double], ys: Seq[DegreesCelsius]): (Double) => DegreesCelsius = {
    val f = LinearInterpolator(DenseVector(xs:_*), DenseVector(ys.map(_.value):_*))
    (d: Double) => DegreesCelsius(f(d))
  }
}
