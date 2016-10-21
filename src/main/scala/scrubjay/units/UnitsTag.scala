package scrubjay.units

import scrubjay.metabase.MetaDescriptor._

import scala.reflect.ClassTag

object UnitsTag {

  object DomainType extends Enumeration {
    type DomainType = Value
    val POINT, MULTIPOINT, RANGE, UNKNOWN= Value
  }

}

abstract class UnitsTag[T <: Units[_] : ClassTag] extends Serializable {

  // FIXME: Infer the rawValueClassTag
  val rawValueClassTag: ClassTag[_]
  val domainType: UnitsTag.DomainType.DomainType

  def extract(units: Units[_]): T = units match {
    case t: T => t
    case _ => throw new RuntimeException("Invalid extractor for type!")
  }

  def extractSeq(unitsSeq: Seq[Units[_]]): Seq[T] = {
    unitsSeq.map{
      case t: T => t
      case _ => throw new RuntimeException("Invalid extractor for type!")
    }
  }

  def convert(value: Any, metaUnits: MetaUnits): T
  def createGeneralInterpolator(xs: Seq[Double], ys: Seq[Units[_]]): (Double) => Units[_] = {
    createInterpolator(xs, extractSeq(ys))
  }
  def createInterpolator(xs: Seq[Double], ys: Seq[T]): (Double) => T
}

