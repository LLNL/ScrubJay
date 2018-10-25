// Copyright 2018 Lawrence Livermore National Security, LLC and other
// ScrubJay Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

package scrubjay.datasetid.original

import org.apache.spark.sql.DataFrame
import scrubjay.dataspace.DimensionSpace
import scrubjay.schema.{ScrubJaySchema, SparkSchema}


case class CassandraDatasetID(keyspace: String,
                              table: String,
                              originalScrubJaySchema: ScrubJaySchema,
                              sparkSchema: SparkSchema)
  extends OriginalDatasetID("CassandraTable", originalScrubJaySchema) {

  override def validFn: Boolean = true

  override def originalDF: DataFrame = {
    spark.read
      .schema(sparkSchema)
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> keyspace, "table" -> table))
      .load()
  }
}

object CassandraDatasetID {
  // TODO: save to cassandra, Cassandra test spec
}
