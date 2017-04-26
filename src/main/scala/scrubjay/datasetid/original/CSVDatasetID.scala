package scrubjay.datasetid.original

import org.apache.spark.sql.scrubjayunits.ScrubJayUDFParser
import org.apache.spark.sql.{DataFrame, SparkSession}
import scrubjay.datasetid.{DatasetID, ScrubJaySchema, SparkSchema}

case class CSVDatasetID(csvFileName: String,
                        sparkSchema: SparkSchema,
                        scrubJaySchema: ScrubJaySchema,
                        options: Map[String, String] = Map.empty)
  extends OriginalDatasetID {

  override def isValid: Boolean = new java.io.File(csvFileName).exists()

  override def realize: DataFrame = {
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
    dsID.realize
      .write
      .options(options)
      .csv(fileName)
  }
}
