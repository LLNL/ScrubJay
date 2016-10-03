package scrubjay.units

import java.io.Serializable

import scrubjay.meta._
import scrubjay.datasource._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect._
import scala.reflect.runtime.universe._

abstract class Units[T](val value: T)(implicit val tag: WeakTypeTag[T]) extends Serializable


object Units {

  def raw2Units(v: Any, mu: MetaUnits): Units[_] = {
    converterForClassTag.getOrElse(mu.classtag, throw new RuntimeException(s"No available converter for $v to $mu")).convert(v, mu)
  }

  def rawRDDToUnitsRDD(sc: SparkContext, rawRDD: RDD[RawDataRow], metaEntryMap: MetaEntryMap): RDD[DataRow] = {
    val broadcastMetaMap = sc.broadcast(metaEntryMap)
    rawRDD.map(row => row.map{case (k, v) => (k, raw2Units(v, broadcastMetaMap.value.getOrElse(k, UNKNOWN_META_ENTRY).units))}.toMap)
  }
}
