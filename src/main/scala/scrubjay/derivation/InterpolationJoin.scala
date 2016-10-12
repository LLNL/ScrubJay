package scrubjay.derivation

import org.apache.spark.rdd.RDD
import scrubjay.datasource.{DataRow, DataSource}
import scrubjay.meta.MetaDescriptor.DimensionType
import scrubjay.meta.MetaSource
import scrubjay.units.UnitsTag.DomainType

class InterpolationJoin(ds1: DataSource, ds2: DataSource) extends Joiner(ds1, ds2) {

  val validEntries = MetaSource.commonMetaEntries(ds1.metaSource, ds2.metaSource)
    .filter(me =>
      me.units.unitsTag.domainType == DomainType.POINT &&
        me.dimension.dimensionType == DimensionType.CONTINUOUS)
    .toSeq

  def isValid = validEntries.nonEmpty

  def derive: DataSource = {

    new DataSource {

      // Implementations of abstract members
      override lazy val metaSource = ds2.metaSource.withMetaEntries(ds1.metaSource.metaEntryMap)

      // RDD derivation defined here
      override lazy val rdd: RDD[DataRow] = {

        val keyColumns1 = validEntries.flatMap(ds1.metaSource.columnForEntry)
        val keyColumns2 = validEntries.flatMap(ds2.metaSource.columnForEntry)

        // TODO: this

        // Create key
        val keyedRDD1 = ds1.rdd.keyBy(row => keyColumns1.map(row))
        val keyedRDD2 = ds2.rdd.keyBy(row => keyColumns2.map(row))

        // Join
        keyedRDD1.join(keyedRDD2).map { case (k, (v1, v2)) => v1 ++ v2 }
      }
    }
  }
}
