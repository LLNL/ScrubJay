package scrubjay.derivation

import org.apache.spark.rdd.RDD
import scrubjay.datasource._
import scrubjay.metabase.GlobalMetaBase._
import scrubjay.metabase.MetaEntry
import scrubjay.metasource._

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

class NaturalJoin(dsID1: DataSourceID, dsID2: DataSourceID) extends DataSourceID(Seq(dsID1, dsID2))() {

  // Determine columns in common between ds1 and ds2 (matching meta entries)
  def validEntries: Seq[MetaEntry] = MetaSource.commonMetaEntries(dsID1.metaSource, dsID2.metaSource)
    .filter(me => me.units == UNITS_UNORDERED_DISCRETE && me.dimension != DIMENSION_UNKNOWN)
    .toSeq

  def keyColumns1: Seq[String] = validEntries.flatMap(dsID1.metaSource.columnForEntry)
  def keyColumns2: Seq[String] = validEntries.flatMap(dsID2.metaSource.columnForEntry)

  def isValid: Boolean = validEntries.nonEmpty

  val metaSource: MetaSource = dsID2.metaSource.withMetaEntries(dsID1.metaSource)
    .withoutColumns(keyColumns2)

  def realize: ScrubJayRDD = {

    val ds1 = dsID1.realize
    val ds2 = dsID2.realize

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

    new ScrubJayRDD(rdd)
  }
}

object NaturalJoin {
  def apply(dsID1: DataSourceID, dsID2: DataSourceID): Option[DataSourceID] = {
    val derivedID = new NaturalJoin(dsID1, dsID2)
    if (derivedID.isValid)
      Some(derivedID)
    else
      None
  }
}
