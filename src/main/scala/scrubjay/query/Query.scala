package scrubjay.query

import gov.llnl.ConstraintSolver._
import scrubjay.datasetid._
import scrubjay.dataspace.{DataSpace, DimensionSpace}


case class Query(dataSpace: DataSpace,
                 domainSpace: DimensionSpace,
                 valueSpace: DimensionSpace) {

  // Can I derive a datasource from the set of datasources that satisfies my query?
  lazy val dsIDSetSatisfiesQuery: Constraint[DatasetID] = memoize(args => {
    dataSpace.datasets.filter(p = dataset => {
      val datasetDimensions = dataset.scrubJaySchema(dataSpace.dimensionSpace).dimensions
      val domainDimensions = domainSpace.dimensions.map(_.name)
      val valueDimensions = valueSpace.dimensions.map(_.name)

      // TODO: check if domain dimensions and value dimensions are satisfied
      val satisfiesDomains = datasetDimensions.contains(domainDimensions)
      (domainDimensions ++ valueDimensions).forall(datasetDimensions.contains)
    })

    // Fun case: queried meta entries exist in a data source derived from multiple data sources
    /*
    val dsIDMeta = dsIDSet.toSeq.map(_.sparkSchema.values.toSet).reduce(_ union _)
    val metaSatisfied = query.intersect(dsIDMeta).size == query.size

    if (metaSatisfied)
      JoinSpace.joinedSet(Seq(dsIDSet))
    else
      Seq.empty
    */
  })

  def solutions: Iterator[DatasetID] = {
    QuerySpace(dataSpace, domainSpace, valueSpace)
      .allSolutions(dsIDSetSatisfiesQuery)
      .flatMap(_.solutions)
  }

  def allDerivations: Iterator[DatasetID] = {
    ???
  }
}
