package scrubjay.derivation

import scrubjay._
import scrubjay.meta._
import scrubjay.datasource._

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class DimensionalJoin(metaOntology: MetaBase,
                      ds1: DataSource,
                      ds2: DataSource) extends DerivedDataSource(metaOntology) {
  override val defined: Boolean = false
  override val metaEntryMap: MetaMap = null
  override val rdd: RDD[DataRow] = null
}
