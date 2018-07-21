package scrubjay.datasetid.transformation

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, Generator, UnaryExpression}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.scrubjayunits.ContinuousRangeStringUDTObject
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.unsafe.types.UTF8String
import scrubjay.datasetid._
import scrubjay.dataspace.DimensionSpace
import scrubjay.schema.{ScrubJayField, ScrubJaySchema, SparkSchema}

case class ExplodeRange(override val dsID: DatasetID, column: String, interval: Double)
  extends Transformation("ExplodeRange") {

  // Modify column units from range to the units of points within the range
  def newField(dimensionSpace: DimensionSpace): ScrubJayField = {
    val columnField = dsID.scrubJaySchema(dimensionSpace).getField(column)
    val newUnits = columnField.units.subUnits("rangeUnits")
    columnField.copy(units = newUnits).withGeneratedFieldName
  }

  override def scrubJaySchema(dimensionSpace: DimensionSpace = DimensionSpace.unknown): ScrubJaySchema = {
    ScrubJaySchema(
      dsID.scrubJaySchema(dimensionSpace).fields.map{
        case ScrubJayField(domain, `column`, dimension, units) => newField(dimensionSpace)
        case other => other
      }
    )
  }

  override def isValid(dimensionSpace: DimensionSpace = DimensionSpace.unknown): Boolean = {
    val columnUnits = dsID.scrubJaySchema(dimensionSpace).getField(column).units
    val dimensionName = dsID.scrubJaySchema(dimensionSpace).getField(column).dimension
    val dimensionToExplode = dimensionSpace.dimensions.find(_.name == dimensionName)

    if (dimensionToExplode.isDefined)
      dimensionToExplode.get.continuous && columnUnits.name == "range"
    else
      false
  }

  override def realize(dimensionSpace: DimensionSpace): DataFrame = {
    val DF = dsID.realize(dimensionSpace: DimensionSpace)
    DF.withColumn(column, ExplodeRange.dfExpression(DF(column), interval))
      .withColumnRenamed(column, newField(dimensionSpace).name)
  }
}

object ExplodeRange {

  @ExpressionDescription(
    usage = "_FUNC_(expr) - Separates the elements of continuous range `expr` into multiple rows.",
    extended = """
    Examples:
      > SELECT _FUNC_(array(10, 20));
       10
       20
  """)
  case class ExpressionClass(child: Expression, interval: Double)
    extends UnaryExpression with Generator with CodegenFallback with Serializable {

    override def checkInputDataTypes(): TypeCheckResult = {
      if (child.dataType.isInstanceOf[ContinuousRangeStringUDTObject]) {
        TypeCheckResult.TypeCheckSuccess
      } else {
        TypeCheckResult.TypeCheckFailure(
          s"input to function explodeContinuous should extend ContinuousRangeStringUDTObject type, not ${child.dataType}")
      }
    }

    override def elementSchema: SparkSchema = child.dataType match {
      case c: ContinuousRangeStringUDTObject => new StructType().add("col", c.explodedType)
    }

    override def eval(input: InternalRow): TraversableOnce[InternalRow] = child.dataType match {
      case c: ContinuousRangeStringUDTObject => {
        ArrayData
        val inputString = child.eval(input).asInstanceOf[UTF8String]
        c.explodedValues(inputString, interval)
      }
    }

  }

  def dfExpression(column: Column, interval: Double): Column = withExpr { ExpressionClass(column.expr, interval) }
}

