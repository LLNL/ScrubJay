package scrubjay.datasetid.transformation

import com.fasterxml.jackson.annotation.JsonIgnore
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
import scrubjay.query.schema.{ScrubJayColumnSchemaQuery, ScrubJayDimensionSchemaQuery, ScrubJayUnitsQuery}
import scrubjay.schema.{ScrubJayColumnSchema, ScrubJaySchema, SparkSchema}

case class ExplodeRange(override val dsID: DatasetID, column: String, interval: Double)
  extends Transformation("ExplodeRange") {

  // Modify column units from range to the units of points within the range
  def newField: ScrubJayColumnSchema = {
    val columnField = dsID.scrubJaySchema.getColumn(column)
    val newUnits = columnField.units.subUnits("rangeUnits")
    columnField.copy(units = newUnits).withGeneratedColumnName
  }

  @JsonIgnore
  lazy val explodeColumnQuery = ScrubJayColumnSchemaQuery(
    name=Some(column),
    dimension=Some(ScrubJayDimensionSchemaQuery(continuous=Some(true))),
    units=Some(ScrubJayUnitsQuery(name=Some("range")))
  )

  lazy override val columnDependencies: Set[ScrubJayColumnSchemaQuery] = Set(explodeColumnQuery)

  lazy override val scrubJaySchema: ScrubJaySchema = {
    ScrubJaySchema(
      dsID.scrubJaySchema.columns.map {
        case explodeColumn if explodeColumn.matchesQuery(explodeColumnQuery) => newField
        case other => other
      }
    )
  }

  override def realize: DataFrame = {
    val DF = dsID.realize
    DF.withColumn(column, ExplodeRange.dfExpression(DF(column), interval))
      .withColumnRenamed(column, newField.name)
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

