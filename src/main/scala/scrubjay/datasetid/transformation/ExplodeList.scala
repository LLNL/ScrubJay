package scrubjay.datasetid.transformation

import com.fasterxml.jackson.annotation.JsonIgnore
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionDescription, Generator, UnaryExpression}
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame}
import scrubjay.datasetid._
import scrubjay.query.schema.{ScrubJayColumnSchemaQuery, ScrubJayUnitsSchemaQuery}
import scrubjay.schema.{ScrubJayColumnSchema, ScrubJaySchema}

case class ExplodeList(override val dsID: DatasetID, column: String)
  extends Transformation("ExplodeList") {

  // Modify column units from list to whatever was inside the list
  def newField: ScrubJayColumnSchema = {
    val scrubJayColumn = dsID.scrubJaySchema.getColumn(column)
    val newUnits = scrubJayColumn.units.subUnits("listUnits")
    scrubJayColumn.copy(units = newUnits).withGeneratedColumnName
  }

  @JsonIgnore
  lazy val explodeColumnQuery = ScrubJayColumnSchemaQuery(name=Some(column), units=Some(ScrubJayUnitsSchemaQuery(name=Some("list"))))

  lazy override val columnDependencies: Set[ScrubJayColumnSchemaQuery] = Set(explodeColumnQuery)

  override def scrubJaySchemaFn: ScrubJaySchema = {
    ScrubJaySchema(
      dsID.scrubJaySchema.columns.map {
        case explodeColumn if explodeColumnQuery.matches(explodeColumn) => newField
        case other => other
      }
    )
  }

  def validSparkSchema: Boolean = {
    dsID.realize.schema(column).dataType match {
      case ArrayType(_, _) => true
      case MapType(_, _, _) => true
      case _ => false
    }
  }

  override def validFn: Boolean = {
    super.validFn && validSparkSchema
  }

  override def realize: DataFrame = {
    val df = dsID.realize
    df.withColumn(column, ExplodeList.dfExpression(df(column)))
      .withColumnRenamed(column, newField.name)
  }
}


object ExplodeList {

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
