package scrubjay.derivation

import org.apache.spark.rdd.RDD
import scrubjay.datasource._
import scrubjay.metabase.GlobalMetaBase._
import scrubjay.metasource.MetaSource

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

class NaturalJoin(dso1: Option[ScrubJayRDD], dso2: Option[ScrubJayRDD]) extends Joiner(dso1, dso2) {

  // Determine columns in common between ds1 and ds2 (matching meta entries)
  private val validEntries = MetaSource.commonMetaEntries(ds1.metaSource, ds2.metaSource)
    .filter(me => me.units == UNITS_UNORDERED_DISCRETE && me.dimension != DIMENSION_UNKNOWN)
    .toSeq

  private val keyColumns1 = validEntries.flatMap(ds1.metaSource.columnForEntry)
  private val keyColumns2 = validEntries.flatMap(ds2.metaSource.columnForEntry)

  override def isValid: Boolean = validEntries.nonEmpty

  override def derive: ScrubJayRDD = {

    // Implementations of abstract members
    val metaSource = ds2.metaSource.withMetaEntries(ds1.metaSource.metaEntryMap)
      .withoutColumns(keyColumns2)

    // RDD derivation defined here
    val rdd: RDD[DataRow] = {

      // Create key
      val keyedRDD1 = ds1.keyBy(row => keyColumns1.map(row))
      val keyedRDD2 = ds2.keyBy(row => keyColumns2.map(row))
        // Remove keys from values
        .map { case (rk, rv) => (rk, rv.filterNot { case (k, _) => keyColumns2.contains(k) }) }

      // Join
      keyedRDD1.join(keyedRDD2).map { case (_, (v1, v2)) => v1 ++ v2 }
    }

    new ScrubJayRDD(rdd, metaSource)
  }
}
