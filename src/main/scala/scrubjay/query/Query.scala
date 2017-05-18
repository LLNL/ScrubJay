package scrubjay.query

import gov.llnl.ConstraintSolver._
import scrubjay.datasetid._
import scrubjay.datasetid.transformation.ExplodeDiscreteRange
import scrubjay.dataspace.{DataSpace, DimensionSpace}


case class Query(dataSpace: DataSpace,
                 target: ScrubJaySchema) {

  // Can I derive a datasource from the set of datasources that satisfies my query?
  lazy val dsIDSetSatisfiesQuery: Constraint[DatasetID] = memoize(args => {

    // Filter on all datasets in our dataspace
    val singleSolutions = dataSpace.datasets.filter(dataset => {
      dataset.scrubJaySchema(dataSpace.dimensionSpace).containsMatchesFor(target)
    })

    val derivedSolutions = dataSpace.datasets.map(dataset => {
      dataset.scrubJaySchema(dataSpace.dimensionSpace).fieldNames.map(column =>
        ExplodeDiscreteRange(dataset, column)
      )
    })

    singleSolutions

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
