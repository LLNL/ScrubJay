package scrubjay.datasource

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

case class CSVDatasetID(csvFileName: String, schema: Schema, options: Map[String, String] = Map.empty)
  extends DatasetID {

  override def isValid: Boolean = true

  override def realize: DataFrame = {
    SparkSession.builder().getOrCreate()
      .read
      .schema(schema)
      .options(options ++ Map("inferSchema" -> "false"))
      .csv(csvFileName)
  }
}


object CSVDatasetID {

  def saveToCSV(dsID: DatasetID,
                fileName: String,
                wrapperChar: String,
                delimiter: String,
                noneString: String): Unit = {

    val ds = dsID.realize

    println("Not implemented!")
  }
}
