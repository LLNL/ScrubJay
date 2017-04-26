package org.apache.spark.sql.types.scrubjayunits

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructField


object ScrubJayUDFParser {

  def parse(DF: DataFrame): DataFrame = {

    // For each column, if we can convert to a high-level scrubjaytype, convert it
    DF.schema.fields.foldLeft(DF)((newDF, columnSchema) => {
      parseUDFForColumnSchema(newDF, columnSchema)
    })
  }

  // Get parse function (UDF) for the scrubjaytype specified in metadata, if it exists
  def parseUDFForColumnSchema(df: DataFrame, structField: StructField): DataFrame = {
    if (structField.metadata.contains("scrubjay_parser")) {

      val scrubjayParserMetadata = structField.metadata.getMetadata("scrubjay_parser")
      scrubjayParserMetadata.getString("type") match {
        case "LocalDateTimeRangeString" =>
          df.withColumn(structField.name, SJLocalDateTimeRange_String.parseStringUDF(df, structField, scrubjayParserMetadata))
        case "ArrayString" =>
          ArrayStringUDT.parseStringUDF(df, structField, scrubjayParserMetadata)
        case unknownType: String =>
          throw new RuntimeException(s"ScrubJay type $unknownType unknown!")
      }
    }
    else {
      df
    }
  }
}
