// Copyright 2018 Lawrence Livermore National Security, LLC and other
// ScrubJay Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

/**
  * Single entry point for ScrubJay API
  *
  * All functionality here is meant to be externally accessible via `import scrubjay._`
  *
  */

package object scrubjay {

  /**
    * Expose classes
    */

  // Dataspace
  val DataSpace = scrubjay.dataspace.DataSpace
  val DimensionSpace = scrubjay.dataspace.DimensionSpace

  // Query
  val Query = scrubjay.query.Query

  // Schema
  val ScrubJaySchema = scrubjay.schema.ScrubJaySchema
  val ScrubJayField = scrubjay.schema.ScrubJayColumnSchema
  val ScrubJayUnitsField = scrubjay.schema.ScrubJayUnitsSchema

  // Original datasets
  val LocalDatasetID = scrubjay.datasetid.original.LocalDatasetID
  val CSVDatasetID = scrubjay.datasetid.original.CSVDatasetID
  val ParquetDatasetID = scrubjay.datasetid.original.ParquetDatasetID
  val CassandraDatasetID = scrubjay.datasetid.original.CassandraDatasetID

  // Transformations
  val ExplodeList = scrubjay.datasetid.transformation.ExplodeList
  val ExplodeRange = scrubjay.datasetid.transformation.ExplodeRange

  // Combinations
  val NaturalJoin = scrubjay.datasetid.combination.NaturalJoin
  val InterpolationJoin = scrubjay.datasetid.combination.InterpolationJoin
}
