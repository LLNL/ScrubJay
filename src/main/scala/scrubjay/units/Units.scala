package scrubjay.units

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scrubjay.datasource._
import scrubjay.meta._

import scala.reflect.{ClassTag, _}

abstract class Units/*[T <: Units[T] : ClassTag]*/ extends Serializable {
  //def getClassTag = classTag[T]
}

abstract class UnitsConverter[T] {
  def convert(value: Any, metaUnits: MetaDescriptor = GlobalMetaBase.UNITS_UNKNOWN): Units
}

object Units {

  var allClassTags: Map[ClassTag[_], UnitsConverter[_]] = Map(
    classTag[Identifier] -> Identifier.converter,
    classTag[UnitsList[_]] -> UnitsList.converter,
    classTag[Seconds] -> Seconds.converter
  )

  def raw2Units(v: Any, mu: MetaDescriptor): Units = {
    allClassTags.getOrElse(mu.tag, throw new RuntimeException(s"No available converter for $v to $mu")).convert(v, mu)
  }

  def rawRDDToUnitsRDD(sc: SparkContext, rawRDD: RDD[RawDataRow], metaMap: MetaMap): RDD[DataRow] = {
    val broadcastMetaMap = sc.broadcast(metaMap)
    rawRDD.map(row => row.map{case (k, v) => (k, raw2Units(v, broadcastMetaMap.value(k).units))}.toMap)
  }
}
