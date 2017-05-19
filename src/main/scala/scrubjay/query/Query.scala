package scrubjay.query

import gov.llnl.ConstraintSolver._
import scrubjay.datasetid._
import scrubjay.datasetid.transformation.ExplodeDiscreteRange
import scrubjay.dataspace.DataSpace


case class Query(dataSpace: DataSpace,
                 target: ScrubJaySchema) {

  // TODO: remove repeat solutions

  lazy val noDerivationSolutions: Constraint[DatasetID] = memoize(args => {
    // Find datasets satisfying the query with no derivation
    dataSpace.datasets.filter(dataset => {
      dataset.scrubJaySchema(dataSpace.dimensionSpace).satisfiesQuerySchema(target)
    })
  })

  // Can I derive a datasource from the set of datasources that satisfies my query?
  lazy val dsIDSetSatisfiesQuery: Constraint[DatasetID] = memoize(args => {

    // Find all datasets containing all dimensions (but possibly not units)
    val queryDomainDimensions = target.domainDimensions
    val queryValueDimensions = target.valueDimensions
    val satisfiesDimensions = dataSpace.datasets.filter(dataset => {
      dataset.scrubJaySchema(dataSpace.dimensionSpace).containsDomainDimensions(queryDomainDimensions) &&
        dataset.scrubJaySchema(dataSpace.dimensionSpace).containsValueDimensions(queryValueDimensions)
    })

    // Run all derivations and check if their results satisfy the query
    val singleDerivationSolutions = satisfiesDimensions.flatMap(dataset => {
      dataset.scrubJaySchema(dataSpace.dimensionSpace).fieldNames.flatMap(column => {
        val explodeDiscreteRange = ExplodeDiscreteRange(dataset, column)
        if (explodeDiscreteRange.isValid(dataSpace.dimensionSpace)) {
          Some(explodeDiscreteRange)
        } else {
          None
        }
      })
    }).filter(dataset => {
      dataset.scrubJaySchema(dataSpace.dimensionSpace).satisfiesQuerySchema(target)
    })

    val allJoins = JoinSpace.joinedSet(Seq(dataSpace))

    noDerivationSolutions(args) ++ singleDerivationSolutions ++ allJoins

    /*
     */

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
    QuerySpace(dataSpace, target)
      .allSolutions(dsIDSetSatisfiesQuery)
      .flatMap(_.solutions)
  }

  def allDerivations: Iterator[DatasetID] = {
    ???
  }
}
