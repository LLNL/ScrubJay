package org.apache.spark.sql.scrubjayunits

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{Metadata, StructField}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.scrubjayunits.ScrubJayUDFParser._

// TODO: subtypes, e.g. Array[Int]
object ArrayStringUDT {
  def parseStringUDF(df: DataFrame, structField: StructField, scrubjayParserMetadata: Metadata): DataFrame = {
    val delimiter = scrubjayParserMetadata.getStringOption("delimiter").getOrElse(",")
    val parseUDF = udf((s: String) => s.split(delimiter))
    val newDF = df.withColumn(structField.name, parseUDF(df(structField.name)))
    newDF
  }
}
