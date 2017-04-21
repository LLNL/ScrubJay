package scrubjay.dataset

import org.apache.spark.sql.scrubjaytypes.ScrubJayUDFParser
import org.apache.spark.sql.{DataFrame, SparkSession}

case class CSVDatasetID(csvFileName: String, schema: Schema, options: Map[String, String] = Map.empty)
  extends DatasetID {

  override lazy val isValid: Boolean = new java.io.File(csvFileName).exists()

  override def realize: DataFrame = {
    ScrubJayUDFParser.parse(
      SparkSession.builder().getOrCreate()
        .read
        .schema(schema)
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
