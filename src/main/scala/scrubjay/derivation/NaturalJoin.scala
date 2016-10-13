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

class NaturalJoin(dso1: Option[DataSource], dso2: Option[DataSource]) extends Joiner(dso1, dso2) {

  // Determine columns in common between ds1 and ds2 (matching meta entries)
  lazy val validEntries = MetaSource.commonMetaEntries(ds1.metaSource, ds2.metaSource)
    .filter(me => me.units == UNITS_IDENTIFIER && me.dimension != DIMENSION_UNKNOWN)
    .toSeq

  override protected def isValid = validEntries.nonEmpty

  override protected def derive: DataSource = {

    new DataSource {

      // Implementations of abstract members
      override lazy val metaSource = ds2.metaSource.withMetaEntries(ds1.metaSource.metaEntryMap)

      // RDD derivation defined here
      override lazy val rdd: RDD[DataRow] = {

        val keyColumns1 = validEntries.flatMap(ds1.metaSource.columnForEntry)
        val keyColumns2 = validEntries.flatMap(ds2.metaSource.columnForEntry)

        // Create key
        val keyedRDD1 = ds1.rdd.keyBy(row => keyColumns1.map(row))
        val keyedRDD2 = ds2.rdd.keyBy(row => keyColumns2.map(row))
          // Remove keys from values
          .map { case (rk, rv) => (rk, rv.filterNot { case (k, v) => keyColumns2.contains(k) }) }

        // Join
        keyedRDD1.join(keyedRDD2).map { case (k, (v1, v2)) => v1 ++ v2 }
      }
    }
  }
}
