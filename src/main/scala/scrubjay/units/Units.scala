package scrubjay.units

import scrubjay.datasource._

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.language.implicitConversions
import scala.reflect._
import scala.reflect.ClassTag

abstract class Units[T <: Units[T] : ClassTag] extends Serializable {
  def getClassTag = classTag[T]
}

case class Identifier(v: Any) extends Units[Identifier]
case class Seconds(v: Double) extends Units[Seconds]

case class UnitList[T](v: List[T]) extends Units[UnitList[T]]

object Units {

  // Implicit converters for Any to types accepted by units (e.g. Double)
  implicit def any2Double(a: Any): Double = a match {
    case n: Number => n.doubleValue
    case s: String => s.toDouble
    case _ => throw new RuntimeException(s"Cannot cast $a to Double!")
  }

  // TODO: MetaMap as broadcast value
  def keyRaw2KeyUnits(ik: String, iv: Any, it: ClassTag[_], mm: MetaMap): (String, Units[_]) = (ik, iv, it) match {
    // Canonical units
    case (k, v, t) if t == classTag[Identifier] => (k, Identifier(v))
    case (k, v, t) if t == classTag[Seconds] => (k, Seconds(v))

    // Collection units
    case (k, v, t) if t == classTag[UnitList[_]] => v match {
      case l: List[_] => (k, UnitList(l.map(iv1 => keyRaw2KeyUnits(k, iv1, mm(k).units.children.head.tag, mm)._2)))
    }

    // Error
    case (k, v, t) => throw new RuntimeException("Can't find classTag for " + t)
  }

  def rawRDDToUnitsRDD(sc: SparkContext, rawRDD: RDD[RawDataRow], metaMap: MetaMap): RDD[DataRow] = {
    rawRDD.map(row => row.map{case (k, v) => (k, v, metaMap(k).units.tag)}.map{case (k, v, t) => keyRaw2KeyUnits(k, v, t, metaMap)}.toMap)
  }
}
