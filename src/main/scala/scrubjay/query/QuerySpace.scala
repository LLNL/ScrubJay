package scrubjay.query

import gov.llnl.ConstraintSolver.{ArgumentSpace, Arguments}
import scrubjay.datasource.DataSourceID
import scrubjay.metabase.MetaEntry


case class QuerySpace(metaEntries: Set[MetaEntry], dsIDs: Seq[DataSourceID]) extends ArgumentSpace {

  // TODO: optimize the order of choosing
  // 1. only datasources that satisfy part of the query
  // 2. add in additional other datasources one at a time

  override def enumerate: Iterator[Arguments] = {
    // From 1 to N datasources at a time
    (1 until dsIDs.length+1).toIterator.flatMap(
      // For all combinations of size N
      dsIDs.combinations(_).map(c => Seq(metaEntries, c.toSet[DataSourceID]))
    )
  }

}

case class DerivationSpace(dsIDs: Seq[DataSourceID]) extends ArgumentSpace {

  override def enumerate: Iterator[Arguments] = {
    // From 1 to N datasources at a time
    (1 until dsIDs.length+1).toIterator.flatMap(
      // For all combinations of size N
      dsIDs.combinations(_).map(c => Seq(c.toSet[DataSourceID]))
    )
  }

}

