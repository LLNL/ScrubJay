package scrubjay.units

import scrubjay.metabase._
import scrubjay.metabase.MetaDescriptor._
import scrubjay.datasource._
import org.apache.spark.rdd.RDD
import scrubjay.metasource._

import scala.language.existentials

abstract class Units[T] extends Serializable {
  val value: T
  def rawString: String = value.toString
}

// TODO: How to enforce that a Units class for a Continuous dimension should implement Continuous?
trait Continuous {
  def asDouble: Double
}

trait ContinuousRange[P] {
  def min: Units[_]
  def max: Units[_]
  def explode(period: P): Iterator[Units[_]]
}

trait DiscreteRange {
  def explode: Iterator[Units[_]]
}

object Units {

  def raw2Units(v: Any, mu: MetaUnits): Units[_] = {
    mu.unitsTag.convert(v, mu)
  }

  def rawRDDToUnitsRDD(rawRDD: RDD[RawDataRow], metaEntryMap: MetaSource): RDD[DataRow] = {
    val broadcastMetaMap = rawRDD.sparkContext.broadcast(metaEntryMap)
    rawRDD.map(row => row.map {
      case (k, v) => k -> raw2Units(v, broadcastMetaMap.value.getOrElse(k, UNKNOWN_META_ENTRY).units)
    }.toMap)
  }
}
