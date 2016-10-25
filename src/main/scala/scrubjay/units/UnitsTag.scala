package scrubjay.units

import scrubjay.metabase.MetaDescriptor._

import scala.reflect._
import scala.reflect.ClassTag

object UnitsTag {

  object DomainType extends Enumeration {
    type DomainType = Value
    val POINT, QUANTITY, MULTIPOINT, RANGE, UNKNOWN = Value
  }

}

abstract class UnitsTag[T <: Units[_] : ClassTag, R : ClassTag] extends Serializable {

  val rawValueClassTag: ClassTag[_] = classTag[R]
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
  def createInterpolator(xs: Seq[Double], ys: Seq[Units[_]]): (Double) => Units[_] = {
    if (ys.length == 1)
      (d: Double) => ys.head
    else
      createTypedInterpolator(xs, extractSeq(ys))
  }
  def reduce(ys: Seq[Units[_]]): Units[_] = {
    typedReduce(extractSeq(ys))
  }

  protected def createTypedInterpolator(xs: Seq[Double], ys: Seq[T]): (Double) => T
  protected def typedReduce(ys: Seq[T]): T
}

