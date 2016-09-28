package scrubjay.datasource

import org.apache.spark.rdd.RDD
import scrubjay.meta._

abstract class DataSource extends Serializable {
  val metaBase: MetaBase
  val metaSource: MetaSource
  val rdd: RDD[DataRow]

  def reverseMetaEntryMap = metaSource.metaEntryMap.map(_.swap)

  def dimensions: Set[MetaDimension] = metaSource.metaEntryMap.values.map(_.dimension).toSet

  def containsMeta(meta: List[MetaEntry]): Boolean = {
    meta.forall(metaSource.metaEntryMap.values.toSet.contains)
  }
}

abstract class OriginalDataSource extends DataSource {

}

abstract class DerivedDataSource extends DataSource {
  val defined: Boolean
}
