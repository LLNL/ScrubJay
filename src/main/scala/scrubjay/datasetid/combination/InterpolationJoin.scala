package scrubjay.datasetid.combination

import scala.language.existentials
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.scrubjayunits.{Interpolator, RealValued}
import scrubjay.datasetid.DatasetID
import scrubjay.dataspace.DimensionSpace
import scrubjay.schema.ScrubJaySchema

case class InterpolationJoin(override val dsID1: DatasetID, override val dsID2: DatasetID, window: Double)
  extends Combination("InterpolationJoin") {

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

    val orderedJoinFieldName1 = orderedJoinFields1.map(_.name).head // TODO: multiple ordered join fields
    val orderedJoinFieldName2 = orderedJoinFields2.map(_.name).head // TODO: multiple ordered join fields

    val orderedJoinFieldIndex1 = df1.schema.fieldIndex(orderedJoinFieldName1)
    val orderedJoinFieldIndex2 = df2.schema.fieldIndex(orderedJoinFieldName2)

    // Get value with trait RealValued or get something that can be cast to double
    implicit class anyWithReal(a: Any) {
      def getReal: Double = a match {
        case r: RealValued => r.realValue
        case d: Decimal => d.toDouble
        case n: java.lang.Number => n.doubleValue
      }
    }

    // Create keys, including unordered column values and ordered column bins (2 bins offset by window)
    val binMul = 1.0 / (2.0 * window)
    val binnedRdd1 = df1.rdd.flatMap(row => {
      val unorderedJoinFieldValues = unorderedJoinFieldNames1.map(row.getAs[String]).mkString(",")
      val binIndex = row.get(orderedJoinFieldIndex1).getReal * binMul
      val bin1 = unorderedJoinFieldValues + binIndex.toInt
      val bin2 = unorderedJoinFieldValues + (binIndex + 0.5).toInt
      Seq(
        (bin1, row),
        (bin2, row)
      )
    })
    val binnedRdd2 = df2.rdd.flatMap(row => {
      val unorderedJoinFieldValues = unorderedJoinFieldNames2.map(row.getAs[String]).mkString(",")
      val binIndex = row.get(orderedJoinFieldIndex2).getReal * binMul
      val bin1 = unorderedJoinFieldValues + binIndex.toInt
      val bin2 = unorderedJoinFieldValues + (binIndex + 0.5).toInt
      Seq(
        (bin1, row),
        (bin2, row)
      )
    })

    // Create 1 to N mapping from each row in df1 to rows in df2
    // ..also remove existing unordered join fields from df2
    val df2UnorderedIndices = unorderedJoinFieldNames2.map(name => df2.schema.fieldIndex(name))
    val df2IndexIsUnordered = (0 to df2.schema.fields.length).map(df2UnorderedIndices.contains)
    val df2FilterNotUnorderedIndex = (row: Row) =>
      Row.fromSeq(row.toSeq.zip(df2IndexIsUnordered).flatMap{case (c, false) => Some(c); case _ => None})
    val oneToNMapping = binnedRdd1.cogroup(binnedRdd2)
      .flatMap {
      case (_, (l1, l2)) => l1.map(l1row => {
        // Filter out cells that are farther than `window` away
        val l1v = l1row.get(orderedJoinFieldIndex1).getReal
        (l1row, l2.filter(l2row => {
          val l2v = l2row.get(orderedJoinFieldIndex2).getReal
          Math.abs(l1v - l2v) < window
        }))
      })
    }
      // Combine all ds2 rows that map to the same ds1 row, without repeat rows
      .aggregateByKey(Set[Row]())((set, rows) => set ++ rows, (set1, set2) => set1 ++ set2)
      // Inner join, so filter out any rows that have no matches
      .filter{case (row, rowSet) => rowSet.nonEmpty}
      // Remove redundant unordered keys from df2 rows
      .mapValues(_.toArray.map(df2FilterNotUnorderedIndex))

    // DEBUG
    // .map{case (row, rowSet) => Row.fromSeq(row.toSeq :+ rowSet.toArray.mkString(","))}
    // spark.createDataFrame(oneToNMapping, StructType(df1.schema.fields :+ StructField("rowSet", StringType)))

    // Determine new fields and indices of df2
    val df2NewFieldInfo = df2.schema.fields.zipWithIndex.zip(df2IndexIsUnordered).flatMap{case ((c, i), false) => Some((c, i)); case _ => None}
    val df2NewOrderedIndex = df2NewFieldInfo.indexWhere(_._2 == orderedJoinFieldIndex2)
    val df2NewSparkFields = df2NewFieldInfo.map(_._1).patch(df2NewOrderedIndex, Nil, 1)
    val df2NewSJFields = df2NewSparkFields.map(f => dsID2.scrubJaySchema(dimensionSpace).getField(f.name))

    // Determine interpolators for all values in df2
    val df2Interpolators = df2NewSJFields.zip(df2NewSparkFields)
      .map{ case (sjfield, sparkfield) => Interpolator.get(sjfield.units, sparkfield.dataType)}
    val df2InterpolatorsBcast = spark.sparkContext.broadcast(df2Interpolators)

    // Run interpolators on all mapped values
    def projection(row: Row, keyIndex: Int, mappedRows: Array[Row], mappedKeyIndex: Int): Row = {

      val xv = row.get(keyIndex).getReal
      val xs = mappedRows.map(_.get(mappedKeyIndex).getReal)

      val allYs = mappedRows.map(_.toSeq.toArray.patch(mappedKeyIndex, Nil, 1)).transpose

      val interpolationInfo = allYs.map(ys => xs.zip(ys))
        .map(_.groupBy(_._1).map(_._2.head).toSeq)
        .zip(df2InterpolatorsBcast.value)

      val interpolatedValues = interpolationInfo.map{
        case (points, interpolator) => interpolator.interpolate(points, xv)
      }

      Row.fromSeq(row.toSeq ++ interpolatedValues)
    }

    val interpolated = oneToNMapping.map{case (row, mappedRows) =>
      projection(row, orderedJoinFieldIndex1, mappedRows, df2NewOrderedIndex)}

    spark.createDataFrame(interpolated, StructType(df1.schema.fields ++ df2NewSparkFields))
  }
}
