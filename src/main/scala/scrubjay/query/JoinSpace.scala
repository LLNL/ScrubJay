package scrubjay.query

import scrubjay.datasource._
import scrubjay.derivation._
import scrubjay.metabase._
import scrubjay.metabase.MetaDescriptor._
import scrubjay.metasource._
import scrubjay.units.UnitsTag.DomainType


case class JoinSpace(dsID1: DataSourceID, dsID2: DataSourceID) {

  def enumerate: Iterator[DataSourceID] = {

    // Get shared domain dimensions
    val commonDimensions: Iterator[(MetaDimension, MetaEntry, MetaEntry)] = {
      MetaSource.commonDimensionEntries(dsID1.metaSource, dsID2.metaSource)
        .filter(e => Seq(e._2.relationType, e._3.relationType).forall(_ == MetaRelationType.DOMAIN))
    }

    // Try to join on all pairs of shared domain dimensions
    commonDimensions.flatMap{
      case (dimension, me1, me2) => {
        Seq(

          // Natural
          NaturalJoin(dsID1, dsID2).asOption,
          NaturalJoin(dsID2, dsID1).asOption,

          // Explode one, then natural join
          NaturalJoin(dsID2, ExplodeDiscreteRange(dsID1, dsID1.metaSource.columnForEntry(me1).get)).asOption,
          NaturalJoin(dsID1, ExplodeDiscreteRange(dsID2, dsID2.metaSource.columnForEntry(me2).get)).asOption,

          // Interpolative
          InterpolationJoin(dsID1, dsID2, 1000 /* WINDOW SIZE ??? */).asOption,
          InterpolationJoin(dsID2, dsID1, 1000 /* WINDOW SIZE ??? */).asOption,

          // Expl
          InterpolationJoin(dsID1, ExplodeDiscreteRange(dsID2, dsID2.metaSource.columnForEntry(me2).get), 1000 /* WINDOW SIZE ??? */).asOption,
          InterpolationJoin(dsID2, ExplodeDiscreteRange(dsID1, dsID1.metaSource.columnForEntry(me1).get), 1000 /* WINDOW SIZE ??? */).asOption
        ).flatten
      }
    }
  }
}
