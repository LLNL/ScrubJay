package scrubjay.datasetid.original

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scrubjay.datasetid.{ScrubJaySchema, SparkSchema}
import scrubjay.dataspace.DimensionSpace

case class LocalDatasetID(rawData: Seq[Row],
                          sparkSchema: SparkSchema,
                          scrubJaySchema: ScrubJaySchema)
  extends OriginalDatasetID(scrubJaySchema) {

  override def isValid(dimensionSpace: DimensionSpace): Boolean = true

  override def realize(dimensionSpace: DimensionSpace): DataFrame = {
    val spark = SparkSession.builder().getOrCreate()
    val rdd = spark.sparkContext.parallelize(rawData)
    spark.createDataFrame(rdd, sparkSchema)
  }
}

object LocalDatasetID {

}