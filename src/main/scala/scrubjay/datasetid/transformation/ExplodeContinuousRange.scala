package scrubjay.datasetid.transformation

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, Generator, UnaryExpression}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.scrubjayunits.ContinuousRangeStringUDTObject
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.unsafe.types.UTF8String
import scrubjay.datasetid._
import scrubjay.dataspace.DimensionSpace

case class ExplodeContinuousRange(override val dsID: DatasetID, column: String)
  extends Transformation {

  override def scrubJaySchema(dimensionSpace: DimensionSpace = DimensionSpace.empty): ScrubJaySchema = {
    dsID.scrubJaySchema(dimensionSpace)
  }

  override def isValid(dimensionSpace: DimensionSpace = DimensionSpace.empty): Boolean = {
    val dimensionName = dsID.scrubJaySchema(dimensionSpace)(column).dimension
    val dimensionToExplode = dimensionSpace.dimensions.find(_.name == dimensionName)

    if (dimensionToExplode.isDefined)
      dimensionToExplode.get.continuous
    else
      false
  }

  override def realize(dimensionSpace: DimensionSpace): DataFrame = {
    val DF = dsID.realize(dimensionSpace: DimensionSpace)
    DF.withColumn(column, ExplodeContinuousRange.dfExpression(DF(column)))
  }
}

object ExplodeContinuousRange {

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

    override def elementSchema: SparkSchema = {
      val dtype = child.dataType
      dtype match {
        case c: ContinuousRangeStringUDTObject => new StructType().add("col", c.explodedType)
        case _ => throw new RuntimeException("Cannot explode continuous range for type " + dtype)
      }
    }

    override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
      val dtype = child.dataType
      dtype match {
        case c: ContinuousRangeStringUDTObject => {
          ArrayData
          val inputString = child.eval(input).asInstanceOf[UTF8String]
          val rows = c.explodedValues(inputString, 30000)
          rows
          /*
          if (inputArray == null) {
            Nil
          } else {
            val rows = new Array[InternalRow](inputArray.numElements())
            inputArray.foreach(c.explodedType, (i, e) => {
              rows(i) = InternalRow(e)
            })
            rows
          }
          */
        }
      }
    }

  }

  def dfExpression(column: Column): Column = withExpr { ExpressionClass(column.expr) }
}

