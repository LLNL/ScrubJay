package scrubjay.datasetid.original

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scrubjay.datasetid.{ScrubJaySchema, SparkSchema}

case class LocalDatasetID(rawData: Seq[Row], sparkSchema: SparkSchema, scrubJaySchema: ScrubJaySchema)
  extends OriginalDatasetID {

  override def isValid: Boolean = true

  override def realize: DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    val rdd = spark.sparkContext.parallelize(rawData)
    spark.createDataFrame(rdd, sparkSchema)
  }
}

object LocalDatasetID {

}