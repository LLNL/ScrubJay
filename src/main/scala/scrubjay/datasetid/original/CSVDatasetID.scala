package scrubjay.datasetid.original

import org.apache.spark.sql.{DataFrame, SparkSession}
import scrubjay.datasetid._
import scrubjay.dataspace.DimensionSpace
import scrubjay.schema.{ScrubJaySchema, SparkSchema}

case class CSVDatasetID(csvFileName: String,
                        sparkSchema: SparkSchema,
                        originalScrubJaySchema: ScrubJaySchema,
                        options: Map[String, String] = Map.empty)
  extends OriginalDatasetID("CSV", originalScrubJaySchema) {

  override def isValid: Boolean = {
    new java.io.File(csvFileName).exists()
  }

  override def originalDF: DataFrame = {
    spark.read
      .format("com.databricks.spark.csv")
      .schema(sparkSchema)
      .options(options)
      .load(csvFileName)
  }
}

object CSVDatasetID {

  def saveDataToCSV(dsID: DatasetID,
                    fileName: String,
                    options: Map[String, String]): Unit = {
    dsID.realize
      .write
      .options(options)
      .csv(fileName)
  }

  def generateSkeletonFor(filename: String, header: Boolean = true, delimiter: String = ",", nullValue: String = ""): DatasetID = {
    val headerString = if (header) "true" else "false"
    val spark = SparkSession.builder().getOrCreate()
    val csvOptions = Map(
      "header" -> headerString,
      "delimiter" -> delimiter,
      "nullValue" -> nullValue
    )
    val rawDF = spark.read
      .options(Map("inferSchema" -> "true") ++ csvOptions)
      .csv(filename)

    val sparkSchema = rawDF.schema

    val originalScrubJaySchema = ScrubJaySchema.unknown(sparkSchema)

    CSVDatasetID(filename, sparkSchema, originalScrubJaySchema, csvOptions)
  }
}
