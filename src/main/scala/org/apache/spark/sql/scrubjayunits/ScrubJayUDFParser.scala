package org.apache.spark.sql.scrubjayunits

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{Metadata, StructField}

object ScrubJayUDFParser {

  // Spark hides metadata's internal Map, so we have to improvise for Map semantics...
  implicit class RichMetaData(metadata: Metadata) {
    def getLongOption(key: String): Option[Long] =
      if(metadata.contains(key)) Some(metadata.getLong(key)) else None
    def getDoubleOption(key: String): Option[Double] =
      if(metadata.contains(key)) Some(metadata.getDouble(key)) else None
    def getBooleanOption(key: String): Option[Boolean] =
      if(metadata.contains(key)) Some(metadata.getBoolean(key)) else None
    def getStringOption(key: String): Option[String] =
      if(metadata.contains(key)) Some(metadata.getString(key)) else None
    def getMetadataOption(key: String): Option[Metadata] =
      if(metadata.contains(key)) Some(metadata.getMetadata(key)) else None
    def getLongArrayOption(key: String): Option[Array[Long]] =
      if(metadata.contains(key)) Some(metadata.getLongArray(key)) else None
    def getDoubleArrayOption(key: String): Option[Array[Double]] =
      if(metadata.contains(key)) Some(metadata.getDoubleArray(key)) else None
    def getBooleanArrayOption(key: String): Option[Array[Boolean]] =
      if(metadata.contains(key)) Some(metadata.getBooleanArray(key)) else None
    def getStringArrayOption(key: String): Option[Array[String]] =
      if(metadata.contains(key)) Some(metadata.getStringArray(key)) else None
    def getMetadataArrayOption(key: String): Option[Array[Metadata]] =
      if(metadata.contains(key)) Some(metadata.getMetadataArray(key)) else None
  }

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
          df.withColumn(structField.name, LocalDateTimeRange.parseStringUDF(df, structField, scrubjayParserMetadata))
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
