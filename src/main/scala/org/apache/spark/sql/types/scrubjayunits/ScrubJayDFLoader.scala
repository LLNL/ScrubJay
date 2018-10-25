// Copyright 2018 Lawrence Livermore National Security, LLC and other
// ScrubJay Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

package org.apache.spark.sql.types.scrubjayunits

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructField
import scrubjay.schema.{ScrubJayColumnSchema, ScrubJaySchema}
import scrubjay.schema.RichDataFrame


object ScrubJayDFLoader {

  def load(DF: DataFrame, originalScrubJaySchema: ScrubJaySchema): DataFrame = {

    // For each column, if we can convert to a high-level scrubjaytype, convert it
    DF.schema.fields.foldLeft(DF)((newDF, sparkSchemaField) => {
      parseUDFForColumnSchema(newDF, sparkSchemaField, originalScrubJaySchema.getColumn(sparkSchemaField.name))
    })
      // And update the spark schema to have scrubjay-formatted names "domain:dimension:units"
      .updateSparkSchemaNames(originalScrubJaySchema)
  }

  // Get parse function (UDF) for the scrubjaytype specified in metadata, if it exists
  def parseUDFForColumnSchema(df: DataFrame,
                              sparkSchemaField: StructField,
                              scrubJaySchemaField: ScrubJayColumnSchema): DataFrame = {

    if (sparkSchemaField.metadata.contains("scrubJayType")) {

      val scrubjayParserMetadata = sparkSchemaField.metadata.getMetadata("scrubJayType")
      scrubjayParserMetadata.getString("type") match {
        case "LocalDateTimeString" =>
          df.withColumn(sparkSchemaField.name, ScrubJayLocalDateTime_String.parseStringUDF(df, sparkSchemaField, scrubjayParserMetadata))
        case "LocalDateTimeRangeString" =>
          df.withColumn(sparkSchemaField.name, ScrubJayLocalDateTimeRange_String.parseStringUDF(df, sparkSchemaField, scrubjayParserMetadata))
        case "ArrayString" =>
          ArrayStringUDT.parseStringUDF(df, sparkSchemaField, scrubjayParserMetadata)
        case unknownType: String =>
          throw new RuntimeException(s"ScrubJay type $unknownType unknown!")
      }
    }
    else {
      df
    }
  }
}
