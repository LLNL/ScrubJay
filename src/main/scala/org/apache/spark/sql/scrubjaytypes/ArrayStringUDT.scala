package org.apache.spark.sql.scrubjaytypes

import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import org.apache.spark.sql.scrubjaytypes.ScrubJayUDFParser._

// TODO: subtypes, e.g. Array[Int]
object ArrayStringUDT {
  def parseStringUDF(metadata: Metadata): UserDefinedFunction = {
    val delimiter = metadata.getStringOption("delimiter").getOrElse(",")
    udf((s: String) => s.split(delimiter))
  }
}
