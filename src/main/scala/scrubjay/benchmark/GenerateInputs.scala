package scrubjay.benchmark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.scrubjayunits.ScrubJayLocalDateTime_String
import scrubjay.datasetid.original.LocalDatasetID
import scrubjay.datasetid.{DatasetID, ScrubJayField, ScrubJaySchema, ScrubJayUnitsField}
import scrubjay.dataspace.{Dimension, DimensionSpace}

import scala.util.Random

object GenerateInputs {

  def spark: SparkSession = SparkSession.builder().getOrCreate()

  val randGen = new Random(100)

  def dimensionSpace: DimensionSpace = {
    DimensionSpace(Array(
      Dimension("time", ordered=true, continuous=true),
      Dimension("node", ordered=false, continuous=false),
      Dimension("flops", ordered = true, continuous = true),
      Dimension("temperature", ordered=true, continuous=true)
    ))
  }

  val SJFieldNode =
    ScrubJayField(
      domain=true,
      name="node",
      dimension="node",
      units=ScrubJayUnitsField("identifier", "POINT", "nearest", "nearest", Map.empty))

  val SJFieldNodeList =
    ScrubJayField(
      domain=true,
      name="nodelist",
      dimension="node",
      units=ScrubJayUnitsField("ArrayString", "MULTIPOINT", "nearest", "nearest",
        Map("listUnits" -> SJFieldNode.units)))

  val SJFieldTimestamp =
    ScrubJayField(
      domain=true,
      name="timestamp",
      dimension="time",
      units=ScrubJayUnitsField("datetimestamp", "POINT", "average", "linear", Map.empty))

  val SJFieldTemperature =
    ScrubJayField(
      domain=false,
      name="temperature",
      dimension="temperature",
      units=ScrubJayUnitsField("temperature", "POINT", "average", "linear", Map.empty))

  val SJFieldFlops =
    ScrubJayField(
      domain=false,
      name="flops",
      dimension="flops",
      units=ScrubJayUnitsField("flops", "POINT", "average", "linear", Map.empty))


  val start: ScrubJayLocalDateTime_String = ScrubJayLocalDateTime_String.deserialize("1988-05-14 00:00:00")

  val timestampUDF: UserDefinedFunction = udf((i: Long) => {
    new ScrubJayLocalDateTime_String(start.value.plusNanos(i * 1000000000L))
  })

  val flopsUDF: UserDefinedFunction = udf(() => {
    randGen.nextInt(1000000)
  })

  val temperatureUDF: UserDefinedFunction = udf(() => {
    randGen.nextDouble()*100.0
  })

  def nodeXFlops(numRows: Long): DatasetID = {

    // Create nodes
    val df = spark.range(numRows).toDF("node")
      .withColumn("flops", flopsUDF())

    // Create ScrubJaySchema
    val sjSchema = new ScrubJaySchema(Array(
      SJFieldNode,
      SJFieldFlops
    ))

    LocalDatasetID(df, sjSchema)
  }

  def nodeXTemperature(numRows: Long): DatasetID = {

    // Create nodes
    val df = spark.range(numRows).toDF("node")
      .withColumn("temperature", temperatureUDF())

    // Create ScrubJaySchema
    val sjSchema = new ScrubJaySchema(Array(
      SJFieldNode,
      SJFieldTemperature
    ))

    LocalDatasetID(df, sjSchema)
  }

  def timeXTemp(numRows: Long): DatasetID = {

    // Generate dataframe
    val df = spark.range(numRows).toDF("timestamp")
      .withColumn("timestamp", timestampUDF(col("timestamp")))
      .withColumn("temperature", temperatureUDF())
      .cache

    // Create ScrubJaySchema
    val sjSchema = new ScrubJaySchema(Array(
      SJFieldTimestamp,
      SJFieldTemperature
    ))

    LocalDatasetID(df, sjSchema)
  }

  def timeXFlops(numRows: Long): DatasetID = {

    // Start time at birth
    val start: ScrubJayLocalDateTime_String = ScrubJayLocalDateTime_String.deserialize("1988-05-14 00:00:00")

    // Create timestamps
    val timestampUDF = udf((i: Long) => {
      new ScrubJayLocalDateTime_String(start.value.plusNanos(i * 1000000000L))
    })

    // Create flops
    val flopsUDF = udf(() => {
      randGen.nextInt(1000000)
    })

    // Generate dataframe
    val df = spark.range(numRows).toDF("timestamp")
      .withColumn("timestamp", timestampUDF(col("timestamp")))
      .withColumn("flops", flopsUDF())
      .cache

    // Create ScrubJaySchema
    val sjSchema = new ScrubJaySchema(Array(
      SJFieldTimestamp,
      SJFieldFlops
    ))

    LocalDatasetID(df, sjSchema)
  }
}
