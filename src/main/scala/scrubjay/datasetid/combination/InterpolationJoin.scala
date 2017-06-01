package scrubjay.datasetid.combination

import scala.language.existentials

import org.apache.spark.sql.DataFrame
import scrubjay.datasetid.{DatasetID, ScrubJaySchema}
import scrubjay.dataspace.DimensionSpace

case class InterpolationJoin(override val dsID1: DatasetID, override val dsID2: DatasetID)
  extends Combination {

  def joinedSchema(dimensionSpace: DimensionSpace): Option[ScrubJaySchema] = {
    dsID1.scrubJaySchema(dimensionSpace).joinSchema(dsID2.scrubJaySchema(dimensionSpace))
  }

  override def scrubJaySchema(dimensionSpace: DimensionSpace): ScrubJaySchema = {
    joinedSchema(dimensionSpace)
      .getOrElse(throw new RuntimeException("Invalid schema requested!"))
      .withGeneratedFieldNames
  }

  override def isValid(dimensionSpace: DimensionSpace): Boolean = {
    joinedSchema(dimensionSpace).isDefined
  }

  override def realize(dimensionSpace: DimensionSpace): DataFrame = {
    val df1 = dsID1.realize(dimensionSpace)
    val df2 = dsID2.realize(dimensionSpace)
    val commonColumns = dsID1.scrubJaySchema(dimensionSpace)
      .joinableFields(dsID2.scrubJaySchema(dimensionSpace)).map(_.name)

    df1.join(df2, commonColumns)
  }
}

/*
case class InterpolationJoin(dsID1: DatasetID, dsID2: DatasetID, window: Double)
  extends DatasetID(dsID1, dsID2) {

  // Determine common (point, point) dimension pairs on continuous domains, and all common discrete dimensions
  val commonDimensions: Seq[(MetaDimension, MetaEntry, MetaEntry)] = MetaSource.commonDimensionEntries(dsID1.sparkSchema, dsID2.sparkSchema)
  val commonContinuousDimensions: Seq[(MetaDimension, MetaEntry, MetaEntry)] = commonDimensions.filter(d =>
    d._1.dimensionType == DimensionSpace.CONTINUOUS &&
    d._2.units.unitsTag.domainType == DomainType.POINT &&
    d._2.relationType == MetaRelationType.DOMAIN &&
    d._3.units.unitsTag.domainType == DomainType.POINT &&
    d._3.relationType == MetaRelationType.DOMAIN)
  val commonDiscreteDimensions: Seq[(MetaDimension, MetaEntry, MetaEntry)] = commonDimensions.filter(d =>
    d._1.dimensionType == DimensionSpace.DISCRETE &&
    d._2.relationType == MetaRelationType.DOMAIN &&
    d._3.relationType == MetaRelationType.DOMAIN)

  lazy val continuousDimColumn1: String = commonContinuousDimensions.flatMap { case (_, me1, _) => dsID1.sparkSchema.columnForEntry(me1) }.head
  lazy val continuousDimColumn2: String = commonContinuousDimensions.flatMap { case (_, _, me2) => dsID2.sparkSchema.columnForEntry(me2) }.head

  lazy val discreteDimColumns1: Seq[String] = commonDiscreteDimensions.flatMap { case (_, me1, _) => dsID1.sparkSchema.columnForEntry(me1) }
  lazy val discreteDimColumns2: Seq[String] = commonDiscreteDimensions.flatMap { case (_, _, me2) => dsID2.sparkSchema.columnForEntry(me2) }
  lazy val allDiscreteColumns: Seq[String] = discreteDimColumns1 ++ discreteDimColumns2

  // Single continuous axis for now
  def isValid: Boolean = commonContinuousDimensions.length == 1 && commonDiscreteDimensions.forall(d => {
    d._2.units.unitsTag.domainType == d._3.units.unitsTag.domainType
  })

  lazy val sparkSchema: MetaSource = dsID2.sparkSchema
    .withoutColumns(continuousDimColumn2 +: discreteDimColumns2)
    .withMetaEntries(dsID1.sparkSchema)

  def realize: ScrubJayRDD = {

    val ds1 = dsID1.realize
    val ds2 = dsID2.realize

    val rdd: RDD[DataRow] = {


      // Bin by overlapping bins of size 2*window
      // -- Guarantees that union of bins will contain all points within `window`
      // Key by this bin and all common discrete domain columns
      val windowX2Rcp = 1.0 / (2.0 * window)
      val binnedRdd1 = ds1.flatMap(row => {
        val binIndex = row(continuousDimColumn1).asInstanceOf[Continuous].asDouble * windowX2Rcp
        val discreteColumns = discreteDimColumns1.map(row)
        Seq(
          (discreteColumns :+ binIndex.toInt, row),
          (discreteColumns :+ (binIndex + 0.5).toInt, row)
        )
      })
      val binnedRdd2 = ds2.flatMap(row => {
        val binIndex = row(continuousDimColumn2).asInstanceOf[Continuous].asDouble * windowX2Rcp
        val discreteColumns = discreteDimColumns2.map(row)
        Seq(
          (discreteColumns :+ binIndex.toInt, row),
          (discreteColumns :+ (binIndex + 0.5).toInt, row)
        )
      })

      // Uncomment both lines suffixed **1** to calculate number of mappings for all rows
      // val groupMapping = ds1.sparkContext.longAccumulator("Group Mapping") // **1**

      // Co-group
      val cogrouped = binnedRdd1.cogroup(binnedRdd2)
        // Create 1 to N mapping from ds1 to ds2
        .flatMap {
          case (_, (l1, l2)) => l1.map(l1row => {
          // groupMapping.add(l2.toSeq.length) // **1**
          // Filter out cells that are farther than `window` away
            val l1v = l1row(continuousDimColumn1).asInstanceOf[Continuous].asDouble
            (l1row, l2.filter(l2row => {
              val l2v = l2row(continuousDimColumn2).asInstanceOf[Continuous].asDouble
              Math.abs(l1v - l2v) < window
            }))
          })
        }
        // Combine all ds2 rows that map to the same ds1 row, without repeat rows
        .aggregateByKey(Set[DataRow]())((set, rows) => set ++ rows, (set1, set2) => set1 ++ set2)
        // Remove redundant discrete entries
        .map{case (row, rowSet) => (row, rowSet.map(_.filterNot(kv => allDiscreteColumns.contains(kv._1))))}

      // Lift the heavies here
      val ds2MetaEntries = cogrouped.sparkContext.broadcast(dsID2.sparkSchema)

      def projection(row: DataRow, keyColumn: String, mappedRows: Set[DataRow], mappedKeyColumn: String): DataRow = {

        val observations = scala.collection.mutable.Map[String, (Seq[Double], Seq[Units[_]])]()

        // Add a single observation to the set of observations
        def addObservation(column: String, x: Double, y: Units[_]): Unit = {
          observations.get(column) match {
            case Some((xs, ys)) => observations.update(column, (xs :+ x, ys :+ y))
            case None => observations += (column -> (Seq(x), Seq(y)))
          }
        }

        for (mapRow <- mappedRows; mapElem <- mapRow.filterNot { case (k, _) => k == mappedKeyColumn }) {
          addObservation(mapElem._1, mapRow(mappedKeyColumn).asInstanceOf[Continuous].asDouble, mapElem._2)
        }

        val projectedValues = for (obs <- observations) yield {
          val unitsTag = ds2MetaEntries.value(obs._1).units.unitsTag
          val f = unitsTag.createInterpolator(obs._2._1, obs._2._2)
          obs._1 -> f(row(keyColumn).asInstanceOf[Continuous].asDouble)
        }

        row ++ projectedValues
      }

      cogrouped.map { case (k, l2) => projection(k, continuousDimColumn1, l2, continuousDimColumn2) }
    }

    new ScrubJayRDD(rdd)
  }
}
*/
