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

  // Can I derive a datasource from the set of datasources that satisfies my query?
  lazy val dsIDSetSatisfiesQuery: Constraint[DatasetID] = memoize(args => {
    val dataSpace = args(0).as[DataSpace]
    val queryTarget = args(1).as[ScrubJaySchema]

    // Run all possible joins of this entire set
    val allJoins = JoinSpace(dataSpace, queryTarget).allJoinedDatasets

    // Run all derivations on the joined results
    val allDerivations = allJoins.flatMap(dataset => {
      dataset.scrubJaySchema(dataSpace.dimensionSpace).fieldNames.flatMap(column => {
        val explodeDiscreteRange = ExplodeDiscreteRange(dataset, column)
        if (explodeDiscreteRange.isValid(dataSpace.dimensionSpace)) {
          Some(explodeDiscreteRange)
        } else {
          None
        }
      })
    })

    // Return all joins and derivations that satisfy the query
    (allJoins ++ allDerivations).filter(dataset => {
      dataset.scrubJaySchema(dataSpace.dimensionSpace).satisfiesQuerySchema(queryTarget)
    })
  })

}
