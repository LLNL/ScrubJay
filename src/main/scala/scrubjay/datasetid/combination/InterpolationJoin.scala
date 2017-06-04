package scrubjay.datasetid.combination

import scala.language.existentials
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.scrubjayunits.{Interpolator, RealValued}
import scrubjay.datasetid.{DatasetID, ScrubJaySchema}
import scrubjay.dataspace.DimensionSpace

case class InterpolationJoin(override val dsID1: DatasetID, override val dsID2: DatasetID, window: Double)
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
    joinedSchema(dimensionSpace).isDefined &&
    dsID1.scrubJaySchema(dimensionSpace).joinableFields(dsID2.scrubJaySchema(dimensionSpace))
      // Exactly one join field must be ordered
      // TODO: multiple ordered join fields
      .count(field => dimensionSpace.findDimensionOrDefault(field._1.dimension).ordered) == 1
  }

  override def realize(dimensionSpace: DimensionSpace): DataFrame = {

    val spark = SparkSession.builder().getOrCreate()

    val df1 = dsID1.realize(dimensionSpace)
    val df2 = dsID2.realize(dimensionSpace)
    val joinFields = dsID1.scrubJaySchema(dimensionSpace)
      .joinableFields(dsID2.scrubJaySchema(dimensionSpace))

    // Get ordered and unordered join fields for each dataset
    val (unorderedJoinFields1, unorderedJoinFields2) = {
      joinFields.filterNot(field => dimensionSpace.findDimensionOrDefault(field._1.dimension).ordered)
        .unzip
    }
    val (orderedJoinFields1, orderedJoinFields2) = {
      joinFields.filter(field => dimensionSpace.findDimensionOrDefault(field._1.dimension).ordered)
        .unzip
    }

    // Get field names for join keys
    val unorderedJoinFieldNames1 = unorderedJoinFields1.map(_.name)
    val unorderedJoinFieldNames2 = unorderedJoinFields2.map(_.name)
    val allUnorderedJoinFieldNames = unorderedJoinFieldNames1 ++ unorderedJoinFieldNames2;

    val orderedJoinFieldName1 = orderedJoinFields1.map(_.name).head // TODO: multiple ordered join fields
    val orderedJoinFieldName2 = orderedJoinFields2.map(_.name).head // TODO: multiple ordered join fields

    // Create column "keys" containing array of all join fields
    val binField = StructField("key", StringType)
    val df1BinnedSchema = StructType(binField +: df1.schema.fields)
    val df2BinnedSchema = StructType(binField +: df2.schema.fields)

    // Create keys, including unordered column values and ordered column bins (2 bins offset by window)
    val binMul = 1.0 / (2.0 * window)
    val binnedRdd1 = df1.rdd.flatMap(row => {
      val unorderedJoinFieldValues = unorderedJoinFieldNames1.map(row.getAs[String]).mkString(",")
      val binIndex = row.getAs[RealValued](orderedJoinFieldName1).realValue * binMul
      val bin1 = unorderedJoinFieldValues + binIndex.toInt
      val bin2 = unorderedJoinFieldValues + (binIndex + 0.5).toInt
      Seq(
        (bin1, row),
        (bin2, row)
      )
    })
    val binnedRdd2 = df2.rdd.flatMap(row => {
      val unorderedJoinFieldValues = unorderedJoinFieldNames2.map(row.getAs[String]).mkString(",")
      val binIndex = row.getAs[RealValued](orderedJoinFieldName2).realValue * binMul
      val bin1 = unorderedJoinFieldValues + binIndex.toInt
      val bin2 = unorderedJoinFieldValues + (binIndex + 0.5).toInt
      Seq(
        (bin1, row),
        (bin2, row)
      )
    })

    // Create 1 to N mapping from each row in df1 to rows in df2
    // ..also remove existing unordered join fields from df2
    val removeIndices = unorderedJoinFieldNames2.map(name => df2.schema.fieldIndex(name))
    val rowMask = (0 to df2.schema.fields.length).map(removeIndices.contains)
    val maskRow = (row: Row) =>
      Row.fromSeq(row.toSeq.zip(rowMask).flatMap{case (c, false) => Some(c); case _ => None})
    val oneToNMapping = binnedRdd1.cogroup(binnedRdd2)
      .flatMap {
      case (_, (l1, l2)) => l1.map(l1row => {
        // Filter out cells that are farther than `window` away
        val l1v = l1row.getAs[RealValued](orderedJoinFieldName1).realValue
        (l1row, l2.filter(l2row => {
          val l2v = l2row.getAs[RealValued](orderedJoinFieldName2).realValue
          Math.abs(l1v - l2v) < window
        }))
      })
    }
      // Combine all ds2 rows that map to the same ds1 row, without repeat rows
      .aggregateByKey(Set[Row]())((set, rows) => set ++ rows, (set1, set2) => set1 ++ set2)
      .mapValues(_.toArray.map(maskRow))

    // Determine new fields and indices of df2
    val df2NewFieldInfo = df2.schema.fields.zipWithIndex.zip(rowMask).flatMap{case ((c, i), false) => Some((c, i)); case _ => None}
    val df2OldOrderedIndex = df2.schema.fieldIndex(orderedJoinFieldName2)
    val df2NewOrderedIndex = df2NewFieldInfo.indexWhere(_._2 == df2OldOrderedIndex)
    val df2NewSparkFields = df2NewFieldInfo.map(_._1).patch(df2NewOrderedIndex, Nil, 1)
    val df2NewSJFields = dsID2.scrubJaySchema(dimensionSpace).fields.zip(rowMask).flatMap{case (c, false) => Some(c); case _ => None}

    // Determine interpolators for all values in df2
    val df2Interpolators = df2NewSJFields.zip(df2NewSparkFields)
      .map{ case (sjfield, sparkfield) => Interpolator.get(sjfield.units, sparkfield.dataType)}
    val df2InterpolatorsBcast = spark.sparkContext.broadcast(df2Interpolators)

    // Run interpolators on all mapped values
    def projection(row: Row, keyColumn: String, mappedRows: Array[Row], mappedKeyIndex: Int): Row = {

      val xv = row.getAs[RealValued](keyColumn).realValue
      val xs = mappedRows.map(_.getAs[RealValued](mappedKeyIndex).realValue)

      val allYs = mappedRows.map(_.toSeq.toArray.patch(mappedKeyIndex, Nil, 1)).transpose

      val interpolationInfo = allYs.map(ys => xs.zip(ys))
        .zip(df2InterpolatorsBcast.value)

      val interpolatedValues = interpolationInfo.map{
        case (points, interpolator) => interpolator.interpolate(points, xv)
      }

      Row.fromSeq(row.toSeq ++ interpolatedValues)
    }

    val interpolated = oneToNMapping.map{case (row, mappedRows) =>
      projection(row, orderedJoinFieldName1, mappedRows, df2NewOrderedIndex)}

    spark.createDataFrame(interpolated, StructType(df1.schema.fields ++ df2NewSparkFields))
  }
}
