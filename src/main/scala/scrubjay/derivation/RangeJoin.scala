package scrubjay.derivation

import scrubjay.datasource.{DataRow, ScrubJayRDD}
import scrubjay.metabase.MetaDescriptor.DimensionSpace
import scrubjay.metasource.MetaSource
import scrubjay.units._
import org.apache.spark.rdd.RDD
import scrubjay.units.UnitsTag.DomainType

class RangeJoin(dso1: Option[ScrubJayRDD], dso2: Option[ScrubJayRDD]) extends Joiner(dso1, dso2) {

  // Determine common continuous (point, range) column pairs and and discrete dimension columns
  private val commonDimensions = MetaSource.commonDimensionEntries(ds1.metaSource, ds2.metaSource)
  private val commonContinuousDimensions = commonDimensions.filter(d =>
    d._1.dimensionType == DimensionSpace.CONTINUOUS &&
    d._2.units.unitsTag.domainType == DomainType.RANGE &&
    d._3.units.unitsTag.domainType == DomainType.POINT)
  private val commonDiscreteDimensions = commonDimensions.filter(_._1.dimensionType == DimensionSpace.DISCRETE)

  private val continuousDimColumn1 = commonContinuousDimensions.flatMap { case (_, me1, _) => ds1.metaSource.columnForEntry(me1) }.head
  private val continuousDimColumn2 = commonContinuousDimensions.flatMap { case (_, _, me2) => ds2.metaSource.columnForEntry(me2) }.head

  private val discreteDimColumns1 = commonDiscreteDimensions.flatMap { case (_, me1, _) => ds1.metaSource.columnForEntry(me1) }
  private val discreteDimColumns2 = commonDiscreteDimensions.flatMap { case (_, _, me2) => ds2.metaSource.columnForEntry(me2) }

  // Restrict to single continuous axis for now
  override def isValid: Boolean = commonContinuousDimensions.length == 1

  override def derive: ScrubJayRDD = {

    val metaSource = ds1.metaSource.withMetaEntries(ds2.metaSource.metaEntryMap)
      .withoutColumns(discreteDimColumns2)
      .withoutColumns(Seq(continuousDimColumn2))

    val rdd: RDD[DataRow] = {

      // Cartesian
      val cartesian = ds1.cartesian(ds2)

      // Filter out all pairs where the discrete columns don't match
      val discreteMatch = cartesian.filter { case (row1, row2) => discreteDimColumns1.map(row1) == discreteDimColumns2.map(row2) }

      // Remove redundant discrete entries
      val filteredDiscrete = discreteMatch.map { case (row1, row2) => (row1, row2.filterNot { case (k, _) => discreteDimColumns2.contains(k) } ) }

      // Filter out all pairs where the POINT in ds2 does not reside in the RANGE of ds1
      val continuousMatch = filteredDiscrete.filter { case (row1, row2) =>
        val range = row1(continuousDimColumn1).asInstanceOf[Range]
        val point = row2(continuousDimColumn2).asInstanceOf[Continuous]
        range.minDouble <= point.asDouble && point.asDouble <= range.maxDouble
      }

      // Reduce all points that fall in a range using the appropriate reducer for those units
      val meta = ds1.sparkContext.broadcast(ds2.metaSource.metaEntryMap)
      val reducedMatch = continuousMatch.mapValues(_.toSeq).reduceByKey(_ ++ _)
        .mapValues(_.groupBy(_._1).map(kv => (kv._1, meta.value(kv._1).units.unitsTag.reduce(kv._2.map(_._2)))).toMap[String, Units[_]])

      // Remove redundant continuous entries from row2 and combine results
      reducedMatch.map { case (row1, row2) => row1 ++ row2.filterNot { case (k, _) => k == continuousDimColumn2 } }
    }
    
    new ScrubJayRDD(rdd, metaSource)
  }
}
