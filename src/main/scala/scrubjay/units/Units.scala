package scrubjay.units

import scrubjay.datasource._
import scrubjay.meta._
import scrubjay.units.Identifier._

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect._
import scala.reflect.ClassTag

abstract class Units[T <: Units[T] : ClassTag] extends Serializable {
  def getClassTag = classTag[T]
}

abstract class UnitsConverter[T] {
  def convert(value: Any, metaUnits: MetaDescriptor = GlobalMetaBase.UNITS_UNKNOWN): Units[_]
}

object Units {

  var allClassTags: Map[ClassTag[_], UnitsConverter[_]] = Map(
    classTag[Identifier] -> Identifier.converter,
    classTag[UnitsList[_]] -> UnitsList.converter
  )

  def raw2Units(iv: Any, mu: MetaDescriptor): Units[_] = (iv, mu.tag) match {
    case (v, t) if allClassTags.contains(t) => allClassTags(t).convert(v, mu)
    case (v, t) => throw new RuntimeException("No known converter for " + t)
  }

  def rawRDDToUnitsRDD(sc: SparkContext, rawRDD: RDD[RawDataRow], metaMap: MetaMap): RDD[DataRow] = {
    val broadcastMetaMap = sc.broadcast(metaMap)
    rawRDD.map(row => row.map{case (k, v) => k -> raw2Units(v, broadcastMetaMap.value(k).units)}.toMap)
  }
}
