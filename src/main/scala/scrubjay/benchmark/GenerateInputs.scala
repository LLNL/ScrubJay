package scrubjay.benchmark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.scrubjayunits.ScrubJayLocalDateTime_String
import scrubjay.datasetid.original.LocalDatasetID
import scrubjay.datasetid.{DatasetID, ScrubJayField, ScrubJaySchema, ScrubJayUnitsField}
import scrubjay.dataspace.{Dimension, DimensionSpace}

import scala.util.Random

object GenerateInputs {

  val randGen = new Random(100)

  def spark: SparkSession = SparkSession.builder().getOrCreate()

  def dimensionSpace: DimensionSpace = {
    DimensionSpace(Array(
      Dimension("time", ordered=true, continuous=true),
      Dimension("flops", ordered = true, continuous = true),
      Dimension("temperature", ordered=true, continuous=true)
    ))
  }

  def timeXTemp(numRows: Long): DatasetID = {

    // Start time at birth
    val start: ScrubJayLocalDateTime_String = ScrubJayLocalDateTime_String.deserialize("1988-05-14 00:00:00")

    // Create timestamps
    val timestampUDF = udf((i: Long) => {
      new ScrubJayLocalDateTime_String(start.value.plusNanos(i * 1000000000L))
    })

    // Create flops
    val temperatureUDF = udf(() => {
      randGen.nextDouble()*100.0
    })

    // Generate dataframe
    val df = spark.range(numRows).toDF("timestamp")
      .withColumn("timestamp", timestampUDF(col("timestamp")))
      .withColumn("temperature", temperatureUDF())
      .cache

    // Create ScrubJaySchema
    val sjSchema = new ScrubJaySchema(Array(
      ScrubJayField(
        domain=true,
        name="timestamp",
        dimension="time",
        units=ScrubJayUnitsField("datetimestamp", "POINT", "average", "linear", Map.empty)),
      ScrubJayField(
        domain=false,
        name="temperature",
        dimension="temperature",
        units=ScrubJayUnitsField("temperature", "POINT", "average", "linear", Map.empty))
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
      ScrubJayField(
        domain=true,
        name="timestamp",
        dimension="time",
        units=ScrubJayUnitsField("datetimestamp", "POINT", "average", "linear", Map.empty)),
      ScrubJayField(
        domain=false,
        name="flops",
        dimension="flops",
        units=ScrubJayUnitsField("flops", "POINT", "average", "linear", Map.empty))
    ))

    LocalDatasetID(df, sjSchema)
  }
}
