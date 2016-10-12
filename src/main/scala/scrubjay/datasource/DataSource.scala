package scrubjay.datasource

import org.apache.spark.rdd.RDD
import scrubjay.meta.MetaDescriptor._
import scrubjay.meta._
import scrubjay.units._

abstract class DataSource(rawRdd: RDD[RawDataRow] = null,
                          columns: Seq[String] = Seq.empty,
                          providedMetaSource: MetaSource = new EmptyMetaSource) extends Serializable {


  lazy val metaSource = providedMetaSource.withColumns(columns)
  lazy val rdd: RDD[DataRow] = {
    if (rawRdd == null)
      throw new RuntimeException("DataSource created with `null` RDD input! Perhaps you forgot to `override lazy val rdd`?")
    Units.rawRDDToUnitsRDD(rawRdd, metaSource.metaEntryMap)
  }

  def reverseMetaEntryMap = metaSource.metaEntryMap.map(_.swap)

  def dimensions: Set[MetaDimension] = metaSource.metaEntryMap.values.map(_.dimension).toSet

  def containsMeta(meta: Set[MetaEntry]): Boolean = {
    meta.forall(metaSource.metaEntryMap.values.toSet.contains)
  }
}