package scrubjay.query

import gov.llnl.ConstraintSolver._
import scrubjay.datasetid._
import scrubjay.datasetid.transformation.ExplodeDiscreteRange
import scrubjay.dataspace.DataSpace

case class Query(dataSpace: DataSpace,
                 queryTarget: ScrubJaySchema) {

  // TODO: remove repeat solutions

  def solutions: Iterator[DatasetID] = {
    QuerySpace(dataSpace, queryTarget)
      .allSolutions(Query.dsIDSetSatisfiesQuery)
      .flatMap(_.solutions)
  }

  def allDerivations: Iterator[DatasetID] = {
    ???
  }
}

object Query {
  lazy val noDerivationSolutions: Constraint[DatasetID] = memoize(args => {
    val dataSpace = args(0).as[DataSpace]
    val queryTarget = args(1).as[ScrubJaySchema]

    // Find datasets satisfying the query with no derivation
    dataSpace.datasets.filter(dataset => {
      dataset.scrubJaySchema(dataSpace.dimensionSpace).satisfiesQuerySchema(queryTarget)
    })
  })

  // Can I derive a datasource from the set of datasources that satisfies my query?
  lazy val dsIDSetSatisfiesQuery: Constraint[DatasetID] = memoize(args => {
    val dataSpace = args(0).as[DataSpace]
    val queryTarget = args(1).as[ScrubJaySchema]

    // Find all datasets containing all dimensions (but possibly not units)
    val queryDomainDimensions = queryTarget.domainDimensions
    val queryValueDimensions = queryTarget.valueDimensions
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
      dataset.scrubJaySchema(dataSpace.dimensionSpace).satisfiesQuerySchema(queryTarget)
    })

    val allJoinSolutions = JoinSpace(dataSpace, queryTarget).solutions

    noDerivationSolutions(args) ++ singleDerivationSolutions ++ allJoinSolutions
  })

}
