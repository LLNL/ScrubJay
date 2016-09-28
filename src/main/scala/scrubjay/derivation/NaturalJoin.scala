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

  val ds1IDDimensions = ds1.metaSource.metaEntryMap.filter{case (k, me) => me.units == UNITS_IDENTIFIER}.map{case (k, me) => (me.dimension, (k, me))}
  val ds2IDDimensions = ds2.metaSource.metaEntryMap.filter{case (k, me) => me.units == UNITS_IDENTIFIER}.map{case (k, me) => (me.dimension, (k, me))}
  val commonDimensions = ds1.dimensions.intersect(ds2.dimensions)

  case class dimMap(map: Map[MetaDimension, (String, MetaEntry)]) extends Serializable {
    def columnForDimension(d: MetaDimension) = map(d)._1
    def metaEntryForDimension(d: MetaDimension) = map(d)._2
  }
  val d1Map = dimMap(ds1IDDimensions)
  val d2Map = dimMap(ds2IDDimensions)

  // Implementations of abstract members
  val defined: Boolean = commonDimensions.nonEmpty
  val metaSource = ds2.metaSource.withMetaEntries(ds1.metaSource.metaEntryMap)

  // RDD derivation defined here
  lazy val rdd: RDD[DataRow] = {

    // Get key columns for each datasource
    val d1Columns = commonDimensions.map(d => d1Map.columnForDimension(d))
    val d2Columns = commonDimensions.map(d => d2Map.columnForDimension(d))

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
