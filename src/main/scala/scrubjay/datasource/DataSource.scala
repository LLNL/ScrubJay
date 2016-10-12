package scrubjay.datasource

import org.apache.spark.rdd.RDD
import scrubjay.meta._
import scrubjay.meta.MetaDescriptor._

abstract class DataSource extends Serializable {
  val metaSource: MetaSource
  val rdd: RDD[DataRow]

  def reverseMetaEntryMap = metaSource.metaEntryMap.map(_.swap)

  def dimensions: Set[MetaDimension] = metaSource.metaEntryMap.values.map(_.dimension).toSet

  def containsMeta(meta: Set[MetaEntry]): Boolean = {
    meta.forall(metaSource.metaEntryMap.values.toSet.contains)
  }
}