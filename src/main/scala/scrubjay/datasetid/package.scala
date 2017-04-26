package scrubjay


package object datasetid {
  type SparkSchema = org.apache.spark.sql.types.StructType
  type ScrubJaySchema = Map[String, Map[String, Any]]
}
