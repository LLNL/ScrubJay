package scrubjay

import org.apache.spark.sql.DataFrame

package object datasetid {
  type SparkSchema = org.apache.spark.sql.types.StructType

  implicit class RichDataFrame(df: DataFrame) {
    def updateSparkSchemaNames(scrubJaySchema: ScrubJaySchema): DataFrame = {
      val newSchemaNames = df.schema.map(field => {
        scrubJaySchema(field.name).generateFieldName
      })
      df.toDF(newSchemaNames:_*)
    }
  }

}
