package scrubjay.query

import gov.llnl.ConstraintSolver.{ArgumentSpace, Arguments}
import scrubjay.datasource.DataSourceID
import scrubjay.metabase.MetaEntry


case class QuerySpace(metaEntries: Set[MetaEntry], dsIDs: Seq[DataSourceID]) extends ArgumentSpace {

  // TODO: trim this space!

  val enumeratedDerivations: Seq[DataSourceID] = dsIDs.flatMap(dsID => DerivationSpace.allDerivationChains(Seq(dsID)))

  override def enumerate: Iterator[Arguments] = {

    val originalAndDerivedIDs = dsIDs ++ enumeratedDerivations

    // From 1 to N datasources at a time
    1.to(originalAndDerivedIDs.length).toIterator.flatMap(
      // For all combinations of size N
      originalAndDerivedIDs.combinations(_).map(c => Seq(metaEntries, c.toSet[DataSourceID]))
    )
  }

}

case class DataSourceArgumentSpace(dsIDs: Seq[DataSourceID]) extends ArgumentSpace {

  override def enumerate: Iterator[Arguments] = {
    // From 1 to N datasources at a time
    1.to(dsIDs.length).toIterator.flatMap(
      // For all combinations of size N
      dsIDs.combinations(_).map(c => Seq(c.toSet[DataSourceID]))
    )
  }

}

