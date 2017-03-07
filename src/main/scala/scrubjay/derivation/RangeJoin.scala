package scrubjay.derivation

import scrubjay.datasource._
import scrubjay.metabase.MetaDescriptor.{DimensionSpace, MetaDimension}
import scrubjay.metasource._
import scrubjay.units._
import org.apache.spark.rdd.RDD
import scrubjay.metabase.MetaEntry
import scrubjay.units.UnitsTag.DomainType

case class RangeJoin(dsID1: DataSourceID, dsID2: DataSourceID)
  extends DataSourceID {

  // Determine common continuous (point, range) column pairs and and discrete dimension columns
  val commonDimensions: Seq[(MetaDimension, MetaEntry, MetaEntry)] = MetaSource.commonDimensionEntries(dsID1.metaSource, dsID2.metaSource).toSeq
  val commonContinuousDimensions: Seq[(MetaDimension, MetaEntry, MetaEntry)] = commonDimensions.filter(d =>
    d._1.dimensionType == DimensionSpace.CONTINUOUS &&
    d._2.units.unitsTag.domainType == DomainType.RANGE &&
    d._3.units.unitsTag.domainType == DomainType.POINT)
  val commonDiscreteDimensions: Seq[(MetaDimension, MetaEntry, MetaEntry)] = commonDimensions.filter(_._1.dimensionType == DimensionSpace.DISCRETE)

  val continuousDimColumn1: String = commonContinuousDimensions.flatMap { case (_, me1, _) => dsID1.metaSource.columnForEntry(me1) }.head
  val continuousDimColumn2: String = commonContinuousDimensions.flatMap { case (_, _, me2) => dsID2.metaSource.columnForEntry(me2) }.head

  val discreteDimColumns1: Seq[String] = commonDiscreteDimensions.flatMap { case (_, me1, _) => dsID1.metaSource.columnForEntry(me1) }
  val discreteDimColumns2: Seq[String] = commonDiscreteDimensions.flatMap { case (_, _, me2) => dsID2.metaSource.columnForEntry(me2) }

  // Restrict to single continuous axis for now
  def isValid: Boolean = commonContinuousDimensions.length == 1

  val metaSource: MetaSource = dsID1.metaSource
    .withoutColumns(discreteDimColumns2)
    .withoutColumns(Seq(continuousDimColumn2))
    .withMetaEntries(dsID2.metaSource)

  def realize: ScrubJayRDD = {

    val ds1 = dsID1.realize
    val ds2 = dsID2.realize

    val rdd: RDD[DataRow] = {

      // Cartesian
      val cartesian = ds1.cartesian(ds2)

      // Filter out all pairs where the discrete columns don't match
      val discreteMatch = cartesian.filter { case (row1, row2) => discreteDimColumns1.map(row1) == discreteDimColumns2.map(row2) }

      // Remove redundant discrete entries
      val filteredDiscrete = discreteMatch.map { case (row1, row2) => (row1, row2.filterNot { case (k, _) => discreteDimColumns2.contains(k) } ) }

      // Filter out all pairs where the POINT in ds2 does not reside in the RANGE of ds1
      val continuousMatch = filteredDiscrete.filter { case (row1, row2) =>
        val range = row1(continuousDimColumn1).asInstanceOf[ContinuousRange[_]]
        val point = row2(continuousDimColumn2).asInstanceOf[Continuous]
        range.min.asInstanceOf[Continuous].asDouble <= point.asDouble && point.asDouble <= range.asInstanceOf[Continuous].asDouble
      }

      // Reduce all points that fall in a range using the appropriate reducer for those units
      val meta = ds1.sparkContext.broadcast(dsID2.metaSource)
      val reducedMatch = continuousMatch.mapValues(_.toSeq).reduceByKey(_ ++ _)
        .mapValues(_.groupBy(_._1).map(kv => (kv._1, meta.value(kv._1).units.unitsTag.reduce(kv._2.map(_._2)))).toMap[String, Units[_]])

      // Remove redundant continuous entries from row2 and combine results
      reducedMatch.map { case (row1, row2) => row1 ++ row2.filterNot { case (k, _) => k == continuousDimColumn2 } }
    }
    
    new ScrubJayRDD(rdd)
  }
}

