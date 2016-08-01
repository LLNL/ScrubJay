package scrubjay.units

import scrubjay.datasource._
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

import scala.language.implicitConversions
import scala.reflect._
import scala.reflect.ClassTag

abstract class Units[T <: Units[T] : ClassTag] extends Serializable {
  def getClassTag = classTag[T]
}

case class Identifier(v: Any) extends Units[Identifier]
case class Seconds(v: Double) extends Units[Seconds]
// TODO: Timestamp here

case class UnitsList[T](v: List[T]) extends Units[UnitsList[T]]
// TODO: Rate here

object Units {

  // Implicit converters for Any to types accepted by units (e.g. Double)
  implicit def any2Double(a: Any): Double = a match {
    case n: Number => n.doubleValue
    case s: String => s.toDouble
    case _ => throw new RuntimeException(s"Cannot cast $a to Double!")
  }

  def keyRaw2KeyUnits(ik: String, iv: Any, it: ClassTag[_], mm: Broadcast[MetaMap]): (String, Units[_]) = (ik, iv, it) match {
    // Canonical units
    case (k, v, t) if t == classTag[Identifier] => (k, Identifier(v))
    case (k, v, t) if t == classTag[Seconds] => (k, Seconds(v))

    // Collection units
    case (k, v, t) if t == classTag[UnitsList[_]] => v match {
      case l: List[_] => (k, UnitsList(l.map(iv1 => keyRaw2KeyUnits(k, iv1, mm.value(k).units.children.head.tag, mm)._2)))
    }

    // Error
    case (k, v, t) => throw new RuntimeException("Can't find classTag for " + t)
  }

  def rawRDDToUnitsRDD(sc: SparkContext, rawRDD: RDD[RawDataRow], metaMap: MetaMap): RDD[DataRow] = {
    val broadcastMetaMap = sc.broadcast(metaMap)
    rawRDD.map(row => row.map{case (k, v) => (k, v, broadcastMetaMap.value(k).units.tag)}.map{case (k, v, t) => keyRaw2KeyUnits(k, v, t, broadcastMetaMap)}.toMap)
  }
}
