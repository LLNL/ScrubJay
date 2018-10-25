// Copyright 2018 Lawrence Livermore National Security, LLC and other
// ScrubJay Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

package org.apache.spark.sql.types.scrubjayunits

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{Metadata, StructField}

// TODO: subtypes, e.g. Array[Int]
object ArrayStringUDT {
  def parseStringUDF(df: DataFrame, structField: StructField, scrubjayParserMetadata: Metadata): DataFrame = {
    val delimiter = scrubjayParserMetadata.getElementOrElse("delimiter", ",")
    val parseUDF = udf((s: String) => s.split(delimiter))
    val newDF = df.withColumn(structField.name, parseUDF(df(structField.name)))
    newDF
  }
}
