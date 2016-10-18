package scrubjay.derivation

import breeze.interpolation.LinearInterpolator
import breeze.linalg.DenseVector
import scrubjay.datasource.{DataRow, DataSource}
import scrubjay.metabase.MetaDescriptor.DimensionType
import scrubjay.metasource.MetaSource
import scrubjay.units.UnitsTag.DomainType
import org.apache.spark.rdd.RDD

class InterpolationJoin(dso1: Option[DataSource], dso2: Option[DataSource], window: Double) extends Joiner(dso1, dso2) {

  val validEntries = MetaSource.commonMetaEntries(ds1.metaSource, ds2.metaSource)
    .filter(me =>
      me.units.unitsTag.domainType == DomainType.POINT &&
        me.dimension.dimensionType == DimensionType.CONTINUOUS)
    .toSeq

  // Single axis for now
  def isValid = validEntries.length == 1

  def derive: DataSource = {

    new DataSource {

      // Implementations of abstract members
      override lazy val metaSource = ds2.metaSource.withMetaEntries(ds1.metaSource.metaEntryMap)

      // RDD derivation defined here
      override lazy val rdd: RDD[DataRow] = {

        val keyColumns1 = validEntries.flatMap(ds1.metaSource.columnForEntry).head
        val keyColumns2 = validEntries.flatMap(ds2.metaSource.columnForEntry).head

        // Key each rdd by the window it falls in
        def createKeyForDoubleRow(row: DataRow, keyColumn: String, keyFn: Double => Int) = {
          row(keyColumn).value match {
            case n: Number => keyFn(n.doubleValue)
            case _ => throw new RuntimeException("Value of a continuous dimension is not numerical! Cannot continue!")
          }
        }

        // Key by all values in the current window and next
        val flooredRDD1 = ds1.rdd.keyBy(createKeyForDoubleRow(_, keyColumns1, (d: Double) => scala.math.floor(d/window).toInt))
        val flooredRDD2 = ds2.rdd.keyBy(createKeyForDoubleRow(_, keyColumns2, (d: Double) => scala.math.floor(d/window).toInt))

        // Key by all values in the current window and previous
        val ceiledRDD1 = ds1.rdd.keyBy(createKeyForDoubleRow(_, keyColumns1, (d: Double) => scala.math.ceil(d/window).toInt))
        val ceiledRDD2 = ds2.rdd.keyBy(createKeyForDoubleRow(_, keyColumns2, (d: Double) => scala.math.ceil(d/window).toInt))

        // Group each
        val floorGrouped = flooredRDD1.cogroup(flooredRDD2)
        val ceilGrouped = ceiledRDD1.cogroup(ceiledRDD2)

        // Create 1 to N mapping from ds1 to ds2 for each
        val floorMapped = floorGrouped.flatMap{case (k, (l1, l2)) => l1.map(row => (row, l2))}
        val ceilMapped = ceilGrouped.flatMap{case (k, (l1, l2)) => l1.map(row => (row, l2))}

        // Co-group both window mappings and combine their results
        val grouped = floorMapped.cogroup(ceilMapped).map{case (k, (floored, ceiled)) => (k, floored ++ ceiled)}

        def projection(row: DataRow, keyColumn: String, mappedRows: Seq[DataRow], mappedKeyColumn: String): DataRow = {
          // Create a seq of (x, y) observations
          //   (t=0, "flops"=100 "blorg"=200), (t=1, "flops"=200)
          row
        }

        // Now reduce the N points to get a single interpolated point projected onto ds1
        //val mapping = grouped.map{case (k, l2) => l2.minBy(_.get(keyColumns1))}
        ds1.rdd
      }
    }
  }
}
