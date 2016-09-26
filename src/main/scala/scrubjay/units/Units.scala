package scrubjay.units

import scrubjay.meta._
import scrubjay.datasource._

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.{ClassTag, _}

abstract class Units/*[T <: Units[T] : ClassTag]*/ extends Serializable {
  //def getClassTag = classTag[T]
  val raw: Any
}

abstract class UnitsConverter[T] {
  def convert(value: Any, metaUnits: MetaDescriptor = GlobalMetaBase.UNITS_UNKNOWN): Units
}

object Units {

  var allClassTags: Map[ClassTag[_], UnitsConverter[_]] = Map(
    classTag[Identifier] -> Identifier.converter,
    classTag[UnitsList[_]] -> UnitsList.converter,
    classTag[Seconds] -> Seconds.converter,
    classTag[DateTimeStamp] -> DateTimeStamp.converter,
    classTag[DateTimeSpan] -> DateTimeSpan.converter
  )

  def raw2Units(v: Any, mu: MetaDescriptor): Units = {
    allClassTags.getOrElse(mu.tag, throw new RuntimeException(s"No available converter for $v to $mu")).convert(v, mu)
  }

  def rawRDDToUnitsRDD(sc: SparkContext, rawRDD: RDD[RawDataRow], metaMap: MetaMap): RDD[DataRow] = {
    val unknownMetaEntry = MetaEntry.fromStringTuple("unknown", "unknown", "identifier")
    val broadcastMetaMap = sc.broadcast(metaMap)
    rawRDD.map(row => row.map{case (k, v) => (k, raw2Units(v, broadcastMetaMap.value.getOrElse(k, unknownMetaEntry).units))}.toMap)
  }
}
