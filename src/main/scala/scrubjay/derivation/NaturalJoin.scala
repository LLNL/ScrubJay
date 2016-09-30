package scrubjay.derivation

import org.apache.spark.rdd.RDD
import scrubjay._
import scrubjay.datasource._
import scrubjay.meta.GlobalMetaBase._
import scrubjay.meta._

/*
 * NaturalJoin 
 * 
 * Requirements: 
 *  1. Two input DataSources to derive from
 *  2. Some columns in common between the two (based on their meta entries)
 *
 * Derivation:
 *  The inner join of the two dataSources, based on their common columns
 */

class NaturalJoin(ds1: DataSource,
                  ds2: DataSource,
                  val metaBase: MetaBase) extends DerivedDataSource {

  // Determine columns in common between ds1 and ds2 (matching meta entries)
  val ds1IDDimensions = ds1.metaSource
    .filterEntries(me => me.units == UNITS_IDENTIFIER && me.dimension != DIMENSION_UNKNOWN)
  val ds2IDDimensions = ds2.metaSource
    .filterEntries(me => me.units == UNITS_IDENTIFIER && me.dimension != DIMENSION_UNKNOWN)

  val commonIDDimensions = ds1IDDimensions.values.toSet.intersect(ds2IDDimensions.values.toSet)

  val ds1IDDimensionsKeyed = ds1IDDimensions.map{case (k, me) => (me.dimension, (k, me))}
  val ds2IDDimensionsKeyed = ds2IDDimensions.map{case (k, me) => (me.dimension, (k, me))}

  case class dimMap(map: Map[MetaDimension, (String, MetaEntry)]) extends Serializable {
    def columnForDimension(d: MetaDimension) = map(d)._1
    def metaEntryForDimension(d: MetaDimension) = map(d)._2
  }
  val d1Map = dimMap(ds1IDDimensionsKeyed)
  val d2Map = dimMap(ds2IDDimensionsKeyed)

  // Implementations of abstract members
  val defined: Boolean = commonIDDimensions.nonEmpty
  val metaSource = ds2.metaSource.withMetaEntries(ds1.metaSource.metaEntryMap)

  // RDD derivation defined here
  lazy val rdd: RDD[DataRow] = {

    // Get key columns for each datasource
    val d1Columns = commonIDDimensions.map(me => d1Map.columnForDimension(me.dimension))
    val d2Columns = commonIDDimensions.map(me => d2Map.columnForDimension(me.dimension))

    // Create key
    val keyedRDD1 = ds1.rdd.keyBy(row => d1Columns.map(row))
    val keyedRDD2 = ds2.rdd.keyBy(row => d2Columns.map(row))
      // Remove keys from values
      .map{case (rk, rv) => (rk, rv.filterNot{case (k, v) => d2Columns.contains(k)})}

    // Join
    keyedRDD1.join(keyedRDD2).map{case (k, (v1, v2)) => v1 ++ v2}
  }
}

object NaturalJoin {
  implicit class ScrubJaySession_NaturalJoin(sjs: ScrubJaySession) {
    def deriveNaturalJoin(ds1: DataSource, ds2: DataSource): NaturalJoin = {
      new NaturalJoin(ds1, ds2, sjs.metaBase)
    }
  }
}
