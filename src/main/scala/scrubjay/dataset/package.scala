package scrubjay


package object dataset {
  type SparkSchema = org.apache.spark.sql.types.StructType
  type ScrubJaySchema = Map[String, Map[String, Any]]
}
