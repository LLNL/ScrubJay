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

  def extract(units: Units[_]): T = units.asInstanceOf[T]
  def extractSeq(unitsSeq: Seq[Units[_]]): Seq[T] = unitsSeq.map(extract)

  def convert(value: Any, metaUnits: MetaUnits): T
  def createInterpolator(xs: Seq[Double], ys: Seq[Units[_]]): (Double) => Units[_] = {
    val (nxs, nys) = xs.zip(ys).toMap.toSeq.unzip
    if (ys.length == 1)
      (_: Double) => ys.head
    else
      createTypedInterpolator(nxs, extractSeq(nys))
  }
  def reduce(ys: Seq[Units[_]]): Units[_] = {
    typedReduce(extractSeq(ys))
  }

  protected def createTypedInterpolator(xs: Seq[Double], ys: Seq[T]): (Double) => T
  protected def typedReduce(ys: Seq[T]): T
}

