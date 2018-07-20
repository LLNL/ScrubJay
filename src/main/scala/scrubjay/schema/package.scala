package scrubjay

import org.apache.spark.sql.DataFrame

package object schema {
  type SparkSchema = org.apache.spark.sql.types.StructType

  val UNKNOWN_STRING = "UNKNOWN_STRING"
  val WILDCARD_STRING = "*"
  def wildMatch(s1: String, s2: String): Boolean = {
    s1 == WILDCARD_STRING || s2 == WILDCARD_STRING || s1 == s2
  }

  implicit class RichDataFrame(df: DataFrame) {
    def updateSparkSchemaNames(scrubJaySchema: ScrubJaySchema): DataFrame = {
      val newSchemaNames = df.schema.map(field => {
        scrubJaySchema.getField(field.name).generateFieldName
      })
      df.toDF(newSchemaNames:_*)
    }
  }
}
