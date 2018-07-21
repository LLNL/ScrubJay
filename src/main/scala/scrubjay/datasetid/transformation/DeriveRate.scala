package scrubjay.datasetid.transformation
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag}
import org.apache.spark.sql.types.scrubjayunits.ScrubJayConverter
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{Column, Row, SparkSession}
import scrubjay.datasetid.DatasetID
import scrubjay.dataspace.DimensionSpace
import scrubjay.schema.{ScrubJayField, ScrubJaySchema, ScrubJayUnitsField}

/**
 * Derive finite difference dY/dX for dimensions X and Y using a window of N rows
 */
case class DeriveRate(override val dsID: DatasetID, yDimension: String, xDimension: String, window: Int )
  extends Transformation("DeriveRate") {

  def xFieldOption(dimensionSpace: DimensionSpace) = {
    dsID.scrubJaySchema(dimensionSpace).fields.find(field => field.dimension == xDimension)
  }

  def yFieldOption(dimensionSpace: DimensionSpace) = {
    dsID.scrubJaySchema(dimensionSpace).fields.find(field => field.dimension == yDimension)
  }

  def getXField(dimensionSpace: DimensionSpace): ScrubJayField = xFieldOption(dimensionSpace).get
  def getYField(dimensionSpace: DimensionSpace): ScrubJayField = yFieldOption(dimensionSpace).get

  def getRateFieldName(dimensionSpace: DimensionSpace) = "value:" + getRateDimensionName(dimensionSpace) + ":rate"
  def getRateUnitsName(dimensionSpace: DimensionSpace) = getYField(dimensionSpace).units.name + "_PER_" + getXField(dimensionSpace).units.name
  def getRateDimensionName(dimensionSpace: DimensionSpace) = getYField(dimensionSpace).dimension + "_PER_" + getXField(dimensionSpace).dimension

  override def isValid(dimensionSpace: DimensionSpace = DimensionSpace.unknown): Boolean = {
    val xDimensionExists = dimensionSpace.dimensions.exists(_.name == xDimension)
    val yDimensionExists = dimensionSpace.dimensions.exists(_.name == yDimension)

    if (xDimensionExists && yDimensionExists)
      xFieldOption(dimensionSpace).isDefined && yFieldOption(dimensionSpace).isDefined
    else
      false
  }

  override def scrubJaySchema(dimensionSpace: DimensionSpace) = {

    val xField: ScrubJayField = getXField(dimensionSpace)
    val yField: ScrubJayField = getYField(dimensionSpace)

    val rateSubUnits = Map("numerator" -> yField.units, "denominator" -> xField.units)
    val rateUnits = ScrubJayUnitsField(getRateUnitsName(dimensionSpace), "POINT", "average", "linear", rateSubUnits)

    val rateField = ScrubJayField(domain = false, name = getRateFieldName(dimensionSpace), getRateDimensionName(dimensionSpace), rateUnits)

    new ScrubJaySchema(dsID.scrubJaySchema(dimensionSpace).fields + rateField)
  }

  override def realize(dimensionSpace: DimensionSpace) = {

    val spark = SparkSession.builder().getOrCreate()

    val df = dsID.realize(dimensionSpace)

    val xField: ScrubJayField = xFieldOption(dimensionSpace).get
    val yField: ScrubJayField = yFieldOption(dimensionSpace).get

    val domainColumns: Seq[Column] = scrubJaySchema(dimensionSpace).domainFields
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

    val newSparkSchema = StructType(dfSortedWithLags.schema.fields :+ StructField(getRateFieldName(dimensionSpace), DoubleType))

    spark.createDataFrame(rddWithRate, newSparkSchema)
      .drop(xLagColumn, yLagColumn)
  }
}
