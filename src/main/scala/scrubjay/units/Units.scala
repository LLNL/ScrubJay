package scrubjay.units

import java.io.Serializable

import scrubjay.meta._
import scrubjay.datasource._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

abstract class Units/*[T <: Units[T] : ClassTag]*/ extends Serializable {
  //def getClassTag = classTag[T]
  val raw: Any
}

abstract class UnitsConverter[T] {
  def convert(value: Any, metaUnits: MetaDescriptor = GlobalMetaBase.UNITS_UNKNOWN): Units
}

object Units {

  def raw2Units(v: Any, mu: MetaDescriptor): Units = {
    converterForClassTag.getOrElse(mu.tag, throw new RuntimeException(s"No available converter for $v to $mu")).convert(v, mu)
  }

  def rawRDDToUnitsRDD(sc: SparkContext, rawRDD: RDD[RawDataRow], metaEntryMap: MetaEntryMap): RDD[DataRow] = {
    val broadcastMetaMap = sc.broadcast(metaEntryMap)
    rawRDD.map(row => row.map{case (k, v) => (k, raw2Units(v, broadcastMetaMap.value.getOrElse(k, UNKNOWN_META_ENTRY).units))}.toMap)
  }
}
