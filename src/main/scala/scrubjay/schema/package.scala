package scrubjay

import org.apache.spark.sql.DataFrame

package object schema {
  type SparkSchema = org.apache.spark.sql.types.StructType

  val UNKNOWN_STRING = "UNKNOWN_STRING"

  def wildMatch[T](s1: T, s2: Option[T]): Boolean = {
    s2.isEmpty || s1 == s2.get
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
