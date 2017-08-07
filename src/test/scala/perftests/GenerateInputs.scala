package perftests

import org.apache.spark.sql.types.scrubjayunits.ScrubJayLocalDateTime_String
import scrubjay.datasetid.{DatasetID, ScrubJayField, ScrubJaySchema, ScrubJayUnitsField}
import scrubjay.datasetid.original.LocalDatasetID
import scrubjay.dataspace.{Dimension, DimensionSpace}

import scala.util.Random

object GenerateInputs extends WithSparkSession {

  startSpark()

  val randGen = new Random(100)

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

    // Create numRows timestamps spaced apart by 1 second
    val rows = for (i <- 30L to numRows) yield {
      val t = new ScrubJayLocalDateTime_String(start.value.plusNanos(i * 1000000000L))
      val d = 100.0*randGen.nextDouble()
      (t, d)
    }

    // Create local dataset id
    val df = spark.createDataFrame(rows).toDF("timestamp", "temperature")

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

    // Create numRows timestamps spaced apart by 1 second
    val rows = for (i <- 1L to numRows) yield {
      val t = new ScrubJayLocalDateTime_String(start.value.plusNanos(i * 1000000000L))
      val d = randGen.nextInt(1000000)
      (t, d)
    }

    // Create local dataset id
    val df = spark.createDataFrame(rows).toDF("timestamp", "flops")

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
