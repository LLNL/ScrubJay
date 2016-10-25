package scrubjay.derivation

import scrubjay.DataSource
import scrubjay.datasource.DataRow
import scrubjay.metabase.MetaDescriptor.DimensionType
import scrubjay.metasource.MetaSource
import scrubjay.units._
import org.apache.spark.rdd.RDD
import scrubjay.units.UnitsTag.DomainType

class RangeJoin(dso1: Option[DataSource], dso2: Option[DataSource]) extends Joiner(dso1, dso2) {

  val commonDimensions = MetaSource.commonDimensionEntries(ds1.metaSource, ds2.metaSource)
  val commonContinuousDimensions = commonDimensions.filter(d =>
    d._1.dimensionType == DimensionType.CONTINUOUS &&
    d._2.units.unitsTag.domainType == DomainType.RANGE &&
    d._3.units.unitsTag.domainType == DomainType.POINT)
  val commonDiscreteDimensions = commonDimensions.filter(_._1.dimensionType == DimensionType.DISCRETE)

  // Single continuous axis for now
  def isValid = commonContinuousDimensions.length == 1

  val continuousDimColumn1 = commonContinuousDimensions.flatMap { case (d, me1, me2) => ds1.metaSource.columnForEntry(me1) }.head
  val continuousDimColumn2 = commonContinuousDimensions.flatMap { case (d, me1, me2) => ds2.metaSource.columnForEntry(me2) }.head

  val discreteDimColumns1 = commonDiscreteDimensions.flatMap { case (d, me1, me2) => ds1.metaSource.columnForEntry(me1) }
  val discreteDimColumns2 = commonDiscreteDimensions.flatMap { case (d, me1, me2) => ds2.metaSource.columnForEntry(me2) }

  def derive: DataSource = {

    new DataSource {

      override lazy val metaSource = ds1.metaSource.withMetaEntries(ds2.metaSource.metaEntryMap)
        .withoutColumns(discreteDimColumns2)
        .withoutColumns(Seq(continuousDimColumn2))

      override lazy val rdd: RDD[DataRow] = {


        // Cartesian
        val cartesian = ds1.rdd.cartesian(ds2.rdd)

        // Filter out all pairs where the discrete columns don't match
        val discreteMatch = cartesian.filter { case (row1, row2) => discreteDimColumns1.map(row1) == discreteDimColumns2.map(row2) }


        // Filter out all pairs where the POINT in ds2 does not reside in the RANGE of ds1
        val continuousMatch = discreteMatch.filter { case (row1, row2) => {
          val range = row1(continuousDimColumn1).asInstanceOf[Range]
          val point = row2(continuousDimColumn2).asInstanceOf[Continuous]
          range.minDouble <= point.asDouble && point.asDouble <= range.maxDouble
        }}

        // Remove redundant discrete entries from row2 and combine results
        continuousMatch.map { case (row1, row2) => row1 ++ row2.filterNot {
          case (k, v) => discreteDimColumns2.contains(k) || k == continuousDimColumn2
        }}
      }
    }
  }
}
