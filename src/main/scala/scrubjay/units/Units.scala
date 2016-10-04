package scrubjay.units

import java.io.Serializable

import scrubjay.meta._
import scrubjay.datasource._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

abstract class Units[+T](val value: T) extends Serializable


object Units {

  def raw2Units(v: Any, mu: MetaUnits): Units[_] = {
    mu.unitsTag.convert(v, mu)
  }

  def rawRDDToUnitsRDD(sc: SparkContext, rawRDD: RDD[RawDataRow], metaEntryMap: MetaEntryMap): RDD[DataRow] = {
    val broadcastMetaMap = sc.broadcast(metaEntryMap)
    rawRDD.map(row => row.map{case (k, v) => {
      (k, raw2Units(v, broadcastMetaMap.value.getOrElse(k, UNKNOWN_META_ENTRY).units))
    }})
  }
}
