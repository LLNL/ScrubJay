package scrubjay.datasetid.transformation

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, Generator, UnaryExpression}
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}
import scrubjay.datasetid._
import scrubjay.dataspace.DimensionSpace

case class ExplodeDiscreteRange(override val dsID: DatasetID, column: String)
  extends Transformation {

  // TODO: make units actually composite (right now hacking with name string)
  override def scrubJaySchema(dimensionSpace: DimensionSpace = DimensionSpace.empty): ScrubJaySchema = {
    ScrubJaySchema(
      dsID.scrubJaySchema(dimensionSpace).fields.map{
        // Modify column units from list to whatever was inside the list
        case ScrubJayField(domain, `column`, dimension, units) => {
          val newUnits = ScrubJayUnitsField(units.name.stripPrefix("list<").stripSuffix(">"), units.elementType, units.aggregator, units.interpolator)
          ScrubJayField(domain, column, dimension, newUnits)
        }
        case other => other
      }
    )
  }

  def validScrubJaySchema(dimensionSpace: DimensionSpace = DimensionSpace.empty): Boolean = {
    val columnUnits = dsID.scrubJaySchema(dimensionSpace)(column).units
    columnUnits.name.startsWith("list<") && columnUnits.name.endsWith(">")
  }

  def validSparkSchema(dimensionSpace: DimensionSpace = DimensionSpace.empty): Boolean = {
    dsID.realize(dimensionSpace).schema(column).dataType match {
      case ArrayType(_, _) => true
      case MapType(_, _, _) => true
      case _ => false
    }
  }

  override def isValid(dimensionSpace: DimensionSpace = DimensionSpace.empty): Boolean = {
    validScrubJaySchema(dimensionSpace) && validSparkSchema(dimensionSpace)
  }

  override def realize(dimensionSpace: DimensionSpace = DimensionSpace.empty): DataFrame = {
    val df = dsID.realize(dimensionSpace)
    df.withColumn(column, ExplodeDiscreteRange.dfExpression(df(column)))
      .updateSparkSchemaNames(scrubJaySchema(dimensionSpace))
  }
}


object ExplodeDiscreteRange {

  @ExpressionDescription(
    usage = "_FUNC_(expr) - Separates the elements of array `expr` into multiple rows, or the elements of map `expr` into multiple rows and columns.",
    extended = """
    Examples:
      > SELECT _FUNC_(array(10, 20));
       10
       20
  """)
  case class ExpressionClass(child: Expression)
    extends UnaryExpression with Generator with CodegenFallback with Serializable {

    override def checkInputDataTypes(): TypeCheckResult = {
      if (child.dataType.isInstanceOf[ArrayType] || child.dataType.isInstanceOf[MapType]) {
        TypeCheckResult.TypeCheckSuccess
      } else {
        TypeCheckResult.TypeCheckFailure(
          s"input to function explodeDiscrete should be array or map type, not ${child.dataType}")
      }
    }

    override def elementSchema: StructType = child.dataType match {
      case ArrayType(et, containsNull) =>
        new StructType()
          .add("col", et, containsNull)
      case MapType(kt, vt, valueContainsNull) =>
        new StructType()
          .add("key", kt, nullable = false)
          .add("value", vt, valueContainsNull)
    }

    override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
      child.dataType match {
        case ArrayType(et, _) =>
          val inputArray = child.eval(input).asInstanceOf[ArrayData]
          if (inputArray == null) {
            Nil
          } else {
            val rows = new Array[InternalRow](inputArray.numElements())
            inputArray.foreach(et, (i, e) => {
              rows(i) = InternalRow(e)
            })
            rows
          }
        case MapType(kt, vt, _) =>
          val inputMap = child.eval(input).asInstanceOf[MapData]
          if (inputMap == null) {
            Nil
          } else {
            val rows = new Array[InternalRow](inputMap.numElements())
            var i = 0
            inputMap.foreach(kt, vt, (k, v) => {
              rows(i) = InternalRow(k, v)
              i += 1
            })
            rows
          }
      }
    }

  }

  def dfExpression(column: Column): Column = withExpr { ExpressionClass(column.expr) }
}
