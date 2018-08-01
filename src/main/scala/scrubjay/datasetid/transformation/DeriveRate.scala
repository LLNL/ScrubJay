package scrubjay.datasetid.transformation
import com.fasterxml.jackson.annotation.JsonIgnore
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag}
import org.apache.spark.sql.types.scrubjayunits.ScrubJayConverter
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import scrubjay.datasetid.DatasetID
import scrubjay.schema.{ScrubJayColumnSchema, ScrubJayDimensionSchema, ScrubJaySchema, ScrubJayUnitsSchema}

/**
 * Derive finite difference dY/dX for dimensions X and Y using a window of N rows
 */
case class DeriveRate(override val dsID: DatasetID, yDimension: String, xDimension: String, window: Int )
  extends Transformation("DeriveRate") {

  def xFieldOption = {
    dsID.scrubJaySchema.fields.find(field => field.dimension.name == xDimension)
  }

  def yFieldOption = {
    dsID.scrubJaySchema.fields.find(field => field.dimension.name == yDimension)
  }

  @JsonIgnore
  def getXField: ScrubJayColumnSchema = xFieldOption.get
  @JsonIgnore
  def getYField: ScrubJayColumnSchema = yFieldOption.get

  @JsonIgnore
  def getRateFieldName = "value:" + getRateDimensionName + ":rate"
  @JsonIgnore
  def getRateUnitsName = getYField.units.name + "_PER_" + getXField.units.name
  @JsonIgnore
  def getRateDimensionName = getYField.dimension + "_PER_" + getXField.dimension

  override val scrubJaySchema: ScrubJaySchema = {

    val xField: ScrubJayColumnSchema = getXField
    val yField: ScrubJayColumnSchema = getYField

    val rateSubUnits = Map("numerator" -> yField.units, "denominator" -> xField.units)
    val rateUnits = ScrubJayUnitsSchema(getRateUnitsName, "POINT", "average", "linear", rateSubUnits)

    val rateField = ScrubJayColumnSchema(domain = false, name = getRateFieldName, ScrubJayDimensionSchema(getRateDimensionName), rateUnits)

    new ScrubJaySchema(dsID.scrubJaySchema.fields + rateField)
  }

  override val valid: Boolean = {
    val xDimensionExists = scrubJaySchema.dimensions.exists(_.name == xDimension)
    val yDimensionExists = scrubJaySchema.dimensions.exists(_.name == yDimension)

    if (xDimensionExists && yDimensionExists)
      xFieldOption.isDefined && yFieldOption.isDefined
    else
      false
  }

  override def realize: DataFrame = {

    val spark = SparkSession.builder().getOrCreate()

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

    val newSparkSchema = StructType(dfSortedWithLags.schema.fields :+ StructField(getRateFieldName, DoubleType))

    spark.createDataFrame(rddWithRate, newSparkSchema)
      .drop(xLagColumn, yLagColumn)
  }
}
