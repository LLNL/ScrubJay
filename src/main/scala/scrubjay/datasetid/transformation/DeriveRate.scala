package scrubjay.datasetid.transformation
import scrubjay.dataspace.DimensionSpace
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lag, udf}
import org.apache.spark.sql.types.scrubjayunits.{RealValued, ScrubJayArithmetic, ScrubJayConverter, ScrubJayNumberDoubleConverter}
import scrubjay.datasetid.DatasetID
import scrubjay.schema.{ScrubJayField, ScrubJaySchema, ScrubJayUnitsField}


/**
 * Derive finite difference dY/dX for dimensions X and Y using a window of N rows
 */
case class DeriveRate(override val dsID: DatasetID, yDimension: String, xDimension: String, window: Int )
  extends Transformation {

  def xFieldOption(dimensionSpace: DimensionSpace) = {
    dsID.scrubJaySchema(dimensionSpace).fields.find(field => field.dimension == xDimension)
  }

  def yFieldOption(dimensionSpace: DimensionSpace) = {
    dsID.scrubJaySchema(dimensionSpace).fields.find(field => field.dimension == yDimension)
  }

  def getXField(dimensionSpace: DimensionSpace): ScrubJayField = xFieldOption(dimensionSpace).get
  def getYField(dimensionSpace: DimensionSpace): ScrubJayField = yFieldOption(dimensionSpace).get

  def getRateFieldName(dimensionSpace: DimensionSpace) = getYField(dimensionSpace).name + "_PER_" + getXField(dimensionSpace).name
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

    new ScrubJaySchema(scrubJaySchema(dimensionSpace).fields + rateField)
  }

  override def realize(dimensionSpace: DimensionSpace) = {

    val df = dsID.realize(dimensionSpace)

    val xField: ScrubJayField = xFieldOption(dimensionSpace).get
    val yField: ScrubJayField = yFieldOption(dimensionSpace).get

    val xColumn: String = xField.name
    val yColumn: String = yField.name

    val xIndex = df.schema.fields.indexWhere(field => field.name == xField.name)
    val yIndex = df.schema.fields.indexWhere(field => field.name == yField.name)

    val dfSorted = df.sort(xField.name)

    val firstRow = dfSorted.first()
    val firstXValue = firstRow.get(xIndex)
    val firstYValue = firstRow.get(yIndex)

    val xLagColumn = xField.name + "_LAG"
    val yLagColumn = yField.name + "_LAG"

    val xDeltaColumn = xField.name + "_DELTA"
    val yDeltaColumn = yField.name + "_DELTA"

    val scrubJayArithmeticX = ScrubJayArithmetic.get(df.schema(xColumn).dataType)
    val scrubJayArithmeticY = ScrubJayArithmetic.get(df.schema(yColumn).dataType)

    val deltaUdfX = udf((a: Any, b: Any) => scrubJayArithmeticX.-(a, b))
    val deltaUdfY = udf((a: Any, b: Any) => scrubJayArithmeticY.-(a, b))

    val scrubJayNumericConverterX = ScrubJayConverter.get(df.schema(xColumn).dataType)
    val scrubJayNumericConverterY = ScrubJayConverter.get(df.schema(yColumn).dataType)
    val rateUdf = udf((y: Any, x: Any) => scrubJayNumericConverterY.b2a(scrubJayNumericConverterY.a2b(y) / scrubJayNumericConverterX.a2b(x)))

    dfSorted
      .withColumn(xLagColumn, lag(xColumn, window, firstXValue).over(Window.orderBy(xColumn)))
      .withColumn(yLagColumn, lag(yColumn, window, firstYValue).over(Window.orderBy(xColumn)))
      .withColumn(xDeltaColumn, deltaUdfX(col(xColumn), col(xLagColumn)))
      .withColumn(yDeltaColumn, deltaUdfY(col(yColumn), col(yLagColumn)))
      .withColumn(getRateFieldName(dimensionSpace), rateUdf(col(yDeltaColumn), col(xDeltaColumn)))
      .drop(xLagColumn, xDeltaColumn, yLagColumn, yDeltaColumn)
  }
}
