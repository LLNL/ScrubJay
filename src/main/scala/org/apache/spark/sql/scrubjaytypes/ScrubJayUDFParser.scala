package org.apache.spark.sql.scrubjaytypes

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{Metadata, StructField}

object ScrubJayUDFParser {

  def parse(DF: DataFrame): DataFrame = {

    // For each column, if we can convert to a high-level scrubjaytype, convert it
    DF.schema.fields.foldLeft(DF)((newDF, columnSchema) => {
      val parseUDF = parseUDFForColumnSchema(columnSchema)
      if (parseUDF.isDefined)
        newDF.withColumn(columnSchema.name, parseUDF.get(newDF(columnSchema.name)))
      else
        newDF
    })
  }

  // Get parse function (UDF) for the scrubjaytype specified in metadata, if it exists
  def parseUDFForColumnSchema(structField: StructField): Option[UserDefinedFunction] = {
    if (structField.metadata.contains("scrubjaytype")) {

      structField.metadata.getString("scrubjaytype") match {

        case "LocalDateTimeRangeString" => {
          val dateformat = structField.metadata.getString("dateformat")
          Some(LocalDateTimeRangeStringUDT.parseStringUDF(dateformat))
        }

        case "ArrayString" => {
          val delimiter = structField.metadata.getString("delimiter")
          Some(ArrayStringUDT.parseStringUDF(delimiter))
        }

        case unknownType: String => {
          throw new RuntimeException(s"ScrubJay type $unknownType unknown!")
        }
      }
    }
    else {
      None
    }
  }
}
