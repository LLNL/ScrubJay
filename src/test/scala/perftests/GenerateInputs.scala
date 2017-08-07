package perftests

import java.time.LocalDateTime

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types
import org.apache.spark.sql.types.{StructField, StructType, scrubjayunits}
import org.apache.spark.sql.types.scrubjayunits.ScrubJayLocalDateTime_String
import org.apache.spark.sql.types.scrubjayunits.ScrubJayLocalDateTimeRange_String
import scrubjay.datasetid.{DatasetID, ScrubJayField, ScrubJaySchema, ScrubJayUnitsField}
import scrubjay.datasetid.original.LocalDatasetID

import scala.util.Random

object GenerateInputs extends WithSparkSession {

  startSpark()

  val randGen = new Random(100)

  def timeXTemp(numRows: Long): DatasetID = {

    // Start time at birth
    val start: ScrubJayLocalDateTime_String = ScrubJayLocalDateTime_String.deserialize("1988-05-14 00:00:00")

    // Create numRows timestamps spaced apart by 1 second
    val rows = for (i <- 1L to numRows) yield {
      val t = new ScrubJayLocalDateTime_String(start.value.plusNanos(i * 1000000000L))
      val d = 100.0*randGen.nextDouble()
      (t, d)
    }

    // Create local dataset id
    val df = spark.createDataFrame(rows)

    val sjSchema = new ScrubJaySchema(Array(
      ScrubJayField(
        domain=true,
        name="timestamp",
        dimension="time",
        units=ScrubJayUnitsField("datetimestamp", "POINT", "average", "linear", Map.empty)),
      ScrubJayField(
        domain=true,
        name="temperature",
        dimension="temperature",
        units=ScrubJayUnitsField("temperature", "POINT", "average", "linear", Map.empty))
    ))

    LocalDatasetID(df, sjSchema)
  }
}
