package scrubjay.query

import gov.llnl.ConstraintSolver._
import scrubjay.dataset._


case class Query(dataSources: Set[DatasetID],
                 metaEntries: Set[(String, String)]) {

  // Can I derive a datasource from the set of datasources that satisfies my query?
  lazy val dsIDSetSatisfiesQuery: Constraint[DatasetID] = memoize(args => {
    val query = args(0).as[Set[(String, String)]]
    val dsIDSet = args(1).as[Set[DatasetID]]

    // Fun case: queried meta entries exist in a data source derived from multiple data sources
    /*
    val dsIDMeta = dsIDSet.toSeq.map(_.sparkSchema.values.toSet).reduce(_ union _)
    val metaSatisfied = query.intersect(dsIDMeta).size == query.size

    if (metaSatisfied)
      JoinSpace.joinedSet(Seq(dsIDSet))
    else
      Seq.empty
    */
    ???
  })

  def solutions: Iterator[DatasetID] = {
    //QuerySpace(metaEntries, dataSources.toSeq)
    //  .allSolutions(dsIDSetSatisfiesQuery).flatMap(_.solutions)
    ???
  }

  def allDerivations: Iterator[DatasetID] = {
    DataSourceArgumentSpace(dataSources.toSeq)
      .allSolutions(JoinSpace.joinedSet).flatMap(_.solutions)
  }
}
