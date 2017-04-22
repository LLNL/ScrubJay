package org.apache.spark.sql.scrubjaytypes

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf


object ArrayStringUDT {
  // TODO: subtypes, e.g. Array[Int]
  def parseStringUDF(delimiter: String): UserDefinedFunction = {
    udf((s: String) => s.split(delimiter))
  }
}
