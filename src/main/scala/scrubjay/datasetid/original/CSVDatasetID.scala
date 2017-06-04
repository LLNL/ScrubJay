package scrubjay.datasetid.original

import org.apache.spark.sql.types.scrubjayunits.ScrubJayDFLoader
import org.apache.spark.sql.{DataFrame, SparkSession}
import scrubjay.datasetid._
import scrubjay.dataspace.DimensionSpace

case class CSVDatasetID(csvFileName: String,
                        sparkSchema: SparkSchema,
                        scrubJaySchema: ScrubJaySchema,
                        options: Map[String, String] = Map.empty)
  extends OriginalDatasetID(scrubJaySchema) {

  override def scrubJaySchema(dimensionSpace: DimensionSpace): ScrubJaySchema = {
    scrubJaySchema.withGeneratedFieldNames
  }

  override def isValid(dimensionSpace: DimensionSpace = DimensionSpace.empty): Boolean = {
    new java.io.File(csvFileName).exists()
  }

  override def realize(dimensionSpace: DimensionSpace = DimensionSpace.empty): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    val rawDF = spark.read
        .schema(sparkSchema)
        .options(options)
        .csv(csvFileName)
    ScrubJayDFLoader.load(rawDF, scrubJaySchema)
  }
}

object CSVDatasetID {

  def saveDataToCSV(dsID: DatasetID,
                    fileName: String,
                    options: Map[String, String]): Unit = {
    dsID.realize()
      .write
      .options(options)
      .csv(fileName)
  }
}
