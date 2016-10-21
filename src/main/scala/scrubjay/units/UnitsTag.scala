package scrubjay.units

import scrubjay.metabase.MetaDescriptor._

import scala.reflect.ClassTag

object UnitsTag {

  object DomainType extends Enumeration {
    type DomainType = Value
    val POINT, MULTIPOINT, RANGE, UNKNOWN= Value
  }

}

abstract class UnitsTag[T <: Units[_] : ClassTag] {

  // FIXME: Infer the rawValueClassTag
  val rawValueClassTag: ClassTag[_]
  val domainType: UnitsTag.DomainType.DomainType

  def extract(units: Units[_]): T = units match {
    case t: T => t
    case _ => throw new RuntimeException("Invalid extractor for type!")
  }

  def convert(value: Any, metaUnits: MetaUnits): T
  def createInterpolator(xs: Seq[Double], ys: Seq[T]): (Double) => T
}

