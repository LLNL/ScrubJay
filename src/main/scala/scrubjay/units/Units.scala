package scrubjay.units

import scrubjay.datasource._

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect._
import scala.reflect.ClassTag

abstract class Units[T <: Units[T] : ClassTag] extends Serializable {
  def getClassTag = classTag[T]
}

case class Identifier(v: Any) extends Units[Identifier]
case class Seconds(v: Double) extends Units[Seconds]
case class Timestamp(v: Double) extends Units[Timestamp]

case class UnitList[T](l: List[T]) extends Units[UnitList[T]]

object Units {
  // TODO: Make this less ugly
  def keyRaw2KeyUnits(ik: String, iv: Any, it: ClassTag[_], mm: MetaMap): (String, Units[_]) = (ik, iv, it) match {
    case (k, v, t) if t == classTag[Identifier] => (k, Identifier(v))
    case (k, v, t) if t == classTag[Seconds] => v match {
      case n: Number => (k, Seconds(n.doubleValue))
      case _ => throw new UnsupportedOperationException(s"Bad input $k for class $t")
    }
    case (k, v, t) if t == classTag[Timestamp] => v match {
      case n: Number => (k, Timestamp(n.doubleValue))
      case _ => throw new UnsupportedOperationException(s"Bad input $k for class $t")
    }
    case (k, v, t) if t == classTag[UnitList[_]] => v match {
      case l: List[_] =>
        val childTag = mm(k).units.children.head.tag
        (k, UnitList(l.map(iv1 => keyRaw2KeyUnits(k, iv1, childTag, mm)._2)))
    }
    case (k, v, t) => throw new UnsupportedOperationException("Can't find classTag for " + t)
  }

  def rawRDDToUnitsRDD(sc: SparkContext, rawRDD: RDD[RawDataRow], metaMap: MetaMap): RDD[DataRow] = {
    rawRDD.map(row => row.map{case (k, v) => (k, v, metaMap(k).units.tag)}.map{case (k, v, t) => keyRaw2KeyUnits(k, v, t, metaMap)}.toMap)
  }
}
