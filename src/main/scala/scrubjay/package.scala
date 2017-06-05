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
  type DataSpace = scrubjay.dataspace.DataSpace
  type DimensionSpace = scrubjay.dataspace.DimensionSpace
  type Dimension = scrubjay.dataspace.Dimension

  // Query
  type Query = scrubjay.query.Query

  // Schema
  type ScrubJaySchema = scrubjay.datasetid.ScrubJaySchema
  type ScrubJayField = scrubjay.datasetid.ScrubJayField
  type ScrubJayUnitsField = scrubjay.datasetid.ScrubJayUnitsField

  // Original datasets
  type LocalDatasetID = scrubjay.datasetid.original.LocalDatasetID
  type CSVDatasetID = scrubjay.datasetid.original.CSVDatasetID
  type CassandraDatasetID = scrubjay.datasetid.original.CassandraDatasetID

  // Transformations
  type ExplodeList = scrubjay.datasetid.transformation.ExplodeList
  type ExplodeRange = scrubjay.datasetid.transformation.ExplodeRange

  // Combinations
  type NaturalJoin = scrubjay.datasetid.combination.NaturalJoin
  type InterpolationJoin = scrubjay.datasetid.combination.InterpolationJoin
}
