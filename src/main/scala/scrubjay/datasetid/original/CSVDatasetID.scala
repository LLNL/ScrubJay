package scrubjay.datasetid.original

import org.apache.spark.sql.types.scrubjayunits.ScrubJayUDFParser
import org.apache.spark.sql.{DataFrame, SparkSession}
import scrubjay.datasetid.{DatasetID, ScrubJaySchema, SparkSchema}
import scrubjay.dataspace.DimensionSpace

case class CSVDatasetID(csvFileName: String,
                        sparkSchema: SparkSchema,
                        scrubJaySchema: ScrubJaySchema,
                        options: Map[String, String] = Map.empty)
  extends OriginalDatasetID(scrubJaySchema) {

  override def isValid(dimensionSpace: DimensionSpace = DimensionSpace.empty): Boolean = {
    new java.io.File(csvFileName).exists()
  }

  override def realize(dimensionSpace: DimensionSpace = DimensionSpace.empty): DataFrame = {
    ScrubJayUDFParser.parse(
      SparkSession.builder().getOrCreate()
        .read
        .schema(sparkSchema)
        .options(options)
        .csv(csvFileName)
    )
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
