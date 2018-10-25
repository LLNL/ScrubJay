// Copyright 2018 Lawrence Livermore National Security, LLC and other
// ScrubJay Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

package scrubjay.datasetid.original

import org.apache.spark.sql.{DataFrame, Dataset}
import scrubjay.dataspace.DimensionSpace
import scrubjay.schema.{ScrubJaySchema, SparkSchema}

case class LocalDatasetID(dataframe: DataFrame,
                          originalScrubJaySchema: ScrubJaySchema,
                         sparkSchema: Option[SparkSchema] = None)
  extends OriginalDatasetID("LocalData", originalScrubJaySchema) {

  override def validFn: Boolean = true

  override def originalDF: DataFrame = {
    if (sparkSchema.isDefined) {
      spark.createDataFrame(dataframe.rdd, sparkSchema.get)
    } else {
      dataframe
    }
  }
}

object LocalDatasetID {

}