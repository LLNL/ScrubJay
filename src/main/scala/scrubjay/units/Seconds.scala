package scrubjay.units

import breeze.interpolation.LinearInterpolator
import breeze.linalg.DenseVector

import scrubjay.metabase.MetaDescriptor._
import scrubjay.units.UnitsTag.DomainType
import scrubjay.units.UnitsTag.DomainType.DomainType

case class Seconds(value: Double) extends Units[Double]

object Seconds extends UnitsTag[Seconds, Double] {

  override val domainType: DomainType = DomainType.QUANTITY

  override def convert(value: Any, metaUnits: MetaUnits): Seconds = value match {
    case n: Number => Seconds(n.doubleValue)
    case s: String => s.split(":").toSeq match {
      case Seq(sec: String) => Seconds(sec.toDouble)
      case Seq(min: String, sec: String) => Seconds(min.toDouble*60.0 + sec.toDouble)
      case Seq(hr: String, min: String, sec: String) => Seconds(hr.toDouble*3600 + min.toDouble*60.0 + sec.toDouble)
      case _ => throw new RuntimeException(s"Cannot convert String $s to $metaUnits")
    }
    case v => throw new RuntimeException(s"Cannot convert $v to $metaUnits")
  }

  override protected def createTypedInterpolator(xs: Seq[Double], ys: Seq[Seconds]): (Double) => Seconds = {
    val f = LinearInterpolator(DenseVector(xs:_*), DenseVector(ys.map(_.value):_*))
    (d: Double) => Seconds(f(d))
  }

  override protected def typedReduce(ys: Seq[Seconds]): Seconds = {
    Seconds(ys.foldLeft(0.0)(_ + _.value) / ys.length)
  }
}
