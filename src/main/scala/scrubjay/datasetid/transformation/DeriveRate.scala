// Copyright 2018 Lawrence Livermore National Security, LLC and other
// ScrubJay Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

package scrubjay.datasetid.transformation
import com.fasterxml.jackson.annotation.JsonIgnore
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag}
import org.apache.spark.sql.types.scrubjayunits.ScrubJayConverter
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import scrubjay.datasetid.DatasetID
import scrubjay.query.schema.{ScrubJayColumnSchemaQuery, ScrubJayDimensionSchemaQuery, ScrubJaySchemaQuery}
import scrubjay.schema.{ScrubJayColumnSchema, ScrubJayDimensionSchema, ScrubJaySchema, ScrubJayUnitsSchema}

/**
 * Derive finite difference dY/dX for dimensions X and Y using a window of N rows
 */
case class DeriveRate(override val dsID: DatasetID, yDimension: String, xDimension: String, window: Int = 10 )
  extends Transformation("DeriveRate") {

  def xFieldOption = {
    dsID.scrubJaySchema.columns.find(field => field.dimension.name == xDimension)
  }

  def yFieldOption = {
    dsID.scrubJaySchema.columns.find(field => field.dimension.name == yDimension)
  }

  @JsonIgnore
  def getXField: ScrubJayColumnSchema = xFieldOption.get
  @JsonIgnore
  def getYField: ScrubJayColumnSchema = yFieldOption.get

  lazy override val columnDependencies: Set[ScrubJayColumnSchemaQuery] = Set(
    ScrubJayColumnSchemaQuery(dimension=Some(ScrubJayDimensionSchemaQuery(name=Some(xDimension)))),
    ScrubJayColumnSchemaQuery(dimension=Some(ScrubJayDimensionSchemaQuery(name=Some(yDimension))))
  )

  override def scrubJaySchemaFn: ScrubJaySchema = {

    val xField: ScrubJayColumnSchema = getXField
    val yField: ScrubJayColumnSchema = getYField

    // TODO: an original dataset with subunits will attempt decomposition... need to prevent
    val rateSubUnits = Map("numerator" -> yField.units, "denominator" -> xField.units)
    val rateUnits = ScrubJayUnitsSchema("rate", "POINT", "average", "linear", rateSubUnits)

    val rateField = ScrubJayColumnSchema(
      domain=false,
      name="rate",
      ScrubJayDimensionSchema(
        name="rate",
        subDimensions = Seq(
          yField.dimension,
          xField.dimension)),
      units=rateUnits)

    new ScrubJaySchema(dsID.scrubJaySchema.columns + rateField)
  }

  override def realize: DataFrame = {

    val spark = SparkSession.builder().getOrCreate()

    spark.sparkContext.hadoopConfiguration
    val df = dsID.realize

    val xField: ScrubJayColumnSchema = xFieldOption.get
    val yField: ScrubJayColumnSchema = yFieldOption.get

    val domainColumns: Seq[Column] = scrubJaySchema.domainFields
      .filter(f => f != xField && f != yField)
      .map(f => col(f.name))
      .toSeq

    val xColumn: String = xField.name
    val yColumn: String = yField.name

    val xLagColumn = xField.name + "_LAG"
    val yLagColumn = yField.name + "_LAG"

    val dfSortedWithLags = df
      .withColumn(xLagColumn, lag(xColumn, window).over(Window.orderBy(xColumn).partitionBy(domainColumns:_*)))
      .withColumn(yLagColumn, lag(yColumn, window).over(Window.orderBy(xColumn).partitionBy(domainColumns:_*)))

    val rddSortedWithLags = dfSortedWithLags.rdd

    val xDataType = df.schema(xColumn).dataType
    val yDataType = df.schema(yColumn).dataType

    val xConverter = ScrubJayConverter.get(xDataType)
    val yConverter = ScrubJayConverter.get(yDataType)

    val rddWithRate = rddSortedWithLags.map(row => {
      val xValue = row.getAs[Any](xColumn)
      val yValue = row.getAs[Any](yColumn)

      val xLagValue = if (row.getAs[Any](xLagColumn) != null) row.getAs[Any](xLagColumn) else xValue
      val yLagValue = if (row.getAs[Any](yLagColumn) != null) row.getAs[Any](yLagColumn) else yValue

      val xDelta = xConverter.a2b(xValue) - xConverter.a2b(xLagValue)
      val yDelta = yConverter.a2b(yValue) - yConverter.a2b(yLagValue)
      val rate = yDelta / xDelta

      Row.fromSeq(row.toSeq :+ rate)
    })

    val newSparkSchema = StructType(dfSortedWithLags.schema.fields :+ StructField("rate", DoubleType))

    spark.createDataFrame(rddWithRate, newSparkSchema)
      .drop(xLagColumn, yLagColumn)
  }
}
