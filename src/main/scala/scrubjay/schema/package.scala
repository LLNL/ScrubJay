package scrubjay

import org.apache.spark.sql.DataFrame

package object schema {
  type SparkSchema = org.apache.spark.sql.types.StructType

  val UNKNOWN_STRING = "UNKNOWN_STRING"

  implicit class RichDataFrame(df: DataFrame) {
    def updateSparkSchemaNames(scrubJaySchema: ScrubJaySchema): DataFrame = {
      val newSchemaNames = df.schema.map(field => {
        scrubJaySchema.getColumn(field.name).generateFieldName
      })
      df.toDF(newSchemaNames:_*)
    }
  }
}
