package scrubjay.datasource

import scrubjay.meta._

import org.apache.spark.rdd.RDD

abstract class DataSource(val metaOntology: MetaBase) extends Serializable {
  val metaEntryMap: MetaMap
  val rdd: RDD[DataRow]

  val reverseMetaEntryMap = metaEntryMap.map(_.swap)

  def dimensions: Set[MetaDimension] = metaEntryMap.values.map(_.dimension).toSet

  def containsMeta(meta: List[MetaEntry]): Boolean = {
    meta.forall(metaEntryMap.values.toSet.contains)
  }
}

abstract class OriginalDataSource(metaOntology: MetaBase,
                                  val metaEntryMap: MetaMap) extends DataSource(metaOntology)

abstract class DerivedDataSource(metaOntology: MetaBase) extends DataSource(metaOntology) {
  val defined: Boolean
}
