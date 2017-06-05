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
  val Dimension = scrubjay.dataspace.Dimension

  // Query
  val Query = scrubjay.query.Query

  // Schema
  val ScrubJaySchema = scrubjay.datasetid.ScrubJaySchema
  val ScrubJayField = scrubjay.datasetid.ScrubJayField
  val ScrubJayUnitsField = scrubjay.datasetid.ScrubJayUnitsField

  // Original datasets
  val LocalDatasetID = scrubjay.datasetid.original.LocalDatasetID
  val CSVDatasetID = scrubjay.datasetid.original.CSVDatasetID
  val CassandraDatasetID = scrubjay.datasetid.original.CassandraDatasetID

  // Transformations
  val ExplodeList = scrubjay.datasetid.transformation.ExplodeList
  val ExplodeRange = scrubjay.datasetid.transformation.ExplodeRange

  // Combinations
  val NaturalJoin = scrubjay.datasetid.combination.NaturalJoin
  val InterpolationJoin = scrubjay.datasetid.combination.InterpolationJoin
}
