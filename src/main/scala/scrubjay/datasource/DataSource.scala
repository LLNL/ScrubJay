package scrubjay.datasource

import org.apache.spark.rdd.RDD
import scrubjay.meta._

abstract class DataSource(val metaOntology: MetaBase) extends Serializable {
  var metaEntryMap: MetaMap
  val rdd: RDD[DataRow]

  def reverseMetaEntryMap = metaEntryMap.map(_.swap)

  def dimensions: Set[MetaDimension] = metaEntryMap.values.map(_.dimension).toSet

  def containsMeta(meta: List[MetaEntry]): Boolean = {
    meta.forall(metaEntryMap.values.toSet.contains)
  }
}

abstract class OriginalDataSource(metaOntology: MetaBase,
                                  var metaEntryMap: MetaMap) extends DataSource(metaOntology)

abstract class DerivedDataSource(metaOntology: MetaBase) extends DataSource(metaOntology) {
  val defined: Boolean
}
