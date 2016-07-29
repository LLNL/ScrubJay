package scrubjay.derivation

import scrubjay._
import scrubjay.meta._
import scrubjay.datasource._

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/*
 * NaturalJoin 
 * 
 * Requirements: 
 *  1. Two input DataSources to derive from
 *  2. Some columns in common between the two (based on their meta entries)
 *
 * Derivation:
 *  The inner join of the two datasources, based on their common columns
 */

class NaturalJoin(metaOntology: MetaBase,
                  ds1: DataSource,
                  ds2: DataSource) extends DerivedDataSource(metaOntology) {

  // Determine columns in common between ds1 and ds2 (matching meta entries)
  val commonMetaColumns = ds1.metaMap.values.toSet.intersect(ds2.metaMap.values.toSet)

  // Implementations of abstract members
  val defined: Boolean = commonMetaColumns.nonEmpty
  val metaMap: MetaMap = ds2.metaMap ++ ds1.metaMap

  // rdd derivation defined here
  lazy val rdd: RDD[DataRow] = {

    // going to filter out redundant columns from ds2
    val ds2ColumnsToFilter = ds2.metaMap.filter{case (k, v) => commonMetaColumns.contains(v)}.keySet

    val krdd1 = ds1.rdd.keyBy(row =>
        for (me <- commonMetaColumns) yield row(ds1.columnForMeta(me).get))
    val krdd2 = ds2.rdd.keyBy(row =>
        for (me <- commonMetaColumns) yield row(ds2.columnForMeta(me).get))
      // filter redundant columns from ds2
      .mapValues(row => row.filterNot{case (k,v) => ds2ColumnsToFilter.contains(k)})

    // remove keys created for join
    krdd1.join(krdd2).map{case (k, (v1, v2)) => v1 ++ v2}
  }
}

object NaturalJoin {
  implicit class ScrubJaySession_NaturalJoin(sjs: ScrubJaySession) {
    def deriveNaturalJoin(ds1: DataSource, ds2: DataSource): NaturalJoin = {
      new NaturalJoin(sjs.metaOntology, ds1, ds2)
    }
  }
}
