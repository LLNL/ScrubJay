package scrubjay.derivation

import scrubjay._
import scrubjay.meta._
import scrubjay.meta.MetaBase._
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
 *  The inner join of the two dataSources, based on their common columns
 */


class NaturalJoin(metaOntology: MetaBase,
                  ds1: DataSource,
                  ds2: DataSource,
                  sc: SparkContext) extends DerivedDataSource(metaOntology) {

  // Determine columns in common between ds1 and ds2 (matching meta entries)
  val commonDimensions = ds1.dimensions intersect ds2.dimensions
  val ds1Dimensions = ds1.metaEntryMap.map{case (k, me) => (me.dimension, (k, me))}
  val ds2Dimensions = ds2.metaEntryMap.map{case (k, me) => (me.dimension, (k, me))}

  case class dimMap(map: Map[MetaDimension, (String, MetaEntry)]) extends Serializable {
    def columnForDimension(d: MetaDimension) = map(d)._1
    def metaEntryForDimension(d: MetaDimension) = map(d)._2
  }
  val d1Map = dimMap(ds1Dimensions)
  val d2Map = dimMap(ds2Dimensions)

  // Implementations of abstract members
  val defined: Boolean = commonDimensions.nonEmpty && commonDimensions.forall(ds1Dimensions(_)._2.units == UNITS_IDENTIFIER)
  val metaEntryMap: MetaMap = ds2.metaEntryMap ++ ds1.metaEntryMap

  // rdd derivation defined here
  lazy val rdd: RDD[DataRow] = {

    // going to filter out redundant columns from ds2
    val d1Columns = commonDimensions.map(d => d1Map.columnForDimension(d))
    val d2Columns = commonDimensions.map(d => d2Map.columnForDimension(d))

    val keyedRDD1 = ds1.rdd.keyBy(row => d1Columns.map(row))
    val keyedRDD2 = ds2.rdd.keyBy(row => d2Columns.map(row))
      .map{case (rk, rv) => (rk, rv.filterNot{case (k, v) => d2Columns.contains(k)})}

    // remove keys created for join
    keyedRDD1.join(keyedRDD2).map{case (k, (v1, v2)) => v1 ++ v2}
  }
}

object NaturalJoin {
  implicit class ScrubJaySession_NaturalJoin(sjs: ScrubJaySession) {
    def deriveNaturalJoin(ds1: DataSource, ds2: DataSource): NaturalJoin = {
      new NaturalJoin(sjs.metaOntology, ds1, ds2, sjs.sc)
    }
  }
}
