package scrubjay.query

import gov.llnl.ConstraintSolver._
import scrubjay.datasetid._
import scrubjay.datasetid.transformation.ExplodeList
import scrubjay.dataspace.DataSpace
import scrubjay.schema.{ScrubJaySchema, ScrubJaySchemaQuery}

case class Query(dataSpace: DataSpace,
                 queryTarget: ScrubJaySchemaQuery) {

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
    val queryTarget = args(1).as[ScrubJaySchemaQuery]

    // Run all possible joins of this entire set
    val allJoins = JoinSet.joinedSet(Seq(dataSpace))

    // Run all derivations on the joined results
    val allDerivations = allJoins.flatMap(dataset => {
      dataset.scrubJaySchema(dataSpace.dimensionSpace).fieldNames.flatMap(column => {
        val explodeDiscreteRange = ExplodeList(dataset, column)
        if (explodeDiscreteRange.isValid(dataSpace.dimensionSpace)) {
          Some(explodeDiscreteRange)
        } else {
          None
        }
      })
    })

    // Return all joins and derivations that satisfy the query
    (allJoins ++ allDerivations).filter(dataset => {
      dataset.scrubJaySchema(dataSpace.dimensionSpace).matchesQuery(queryTarget)
    })
  })

}
