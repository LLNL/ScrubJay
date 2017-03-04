package scrubjay.query

import scrubjay.datasource._
import scrubjay.derivation._
import scrubjay.metabase._
import scrubjay.metabase.MetaDescriptor._
import scrubjay.metasource._
import scrubjay.units.UnitsTag.DomainType


case class JoinSpace(dsID1: DataSourceID, dsID2: DataSourceID) {

  def enumerate: Iterator[DataSourceID] = {

    // 1. Do they share a domain dimension?
    val commonDimensions: Iterator[(MetaDimension, MetaEntry, MetaEntry)] = {
      MetaSource.commonDimensionEntries(dsID1.metaSource, dsID2.metaSource)
        .filter(e => Seq(e._2.relationType, e._3.relationType).forall(_ == MetaRelationType.DOMAIN))
    }

    // 2. What are the units of the shared dimensions?
    commonDimensions.flatMap{

      // Discrete, Discrete => Natural Join
      case (_,
      MetaEntry(_, MetaDimension(_, _, DimensionSpace.DISCRETE), _),
      MetaEntry(_, MetaDimension(_, _, DimensionSpace.DISCRETE), _)) => {
        NaturalJoin(dsID1, dsID2).asOption
      }

      // Point, Point => Interpolation Join
      case (_,
      MetaEntry(_, MetaDimension(_, _, DimensionSpace.CONTINUOUS), units1),
      MetaEntry(_, MetaDimension(_, _, DimensionSpace.CONTINUOUS), units2))
        if Seq(units1, units2).forall(_.unitsTag.domainType == DomainType.POINT) => {
        InterpolationJoin(dsID1, dsID2, 1000 /* WINDOW SIZE ??? */ ).asOption
      }

      // Point, Range => explode range, interpolation join
      case (_,
      MetaEntry(_, MetaDimension(_, _, DimensionSpace.CONTINUOUS), units1),
      me2 @ MetaEntry(_, MetaDimension(_, _, DimensionSpace.CONTINUOUS), units2))
        if units1.unitsTag.domainType == DomainType.POINT && units2.unitsTag.domainType == DomainType.RANGE => {
        InterpolationJoin(dsID1, ExplodeList(dsID2, Seq(dsID2.metaSource.columnForEntry(me2)).flatten), 1000 /* WINDOW SIZE ??? */ ).asOption
      }

      // Range, Point => explode range, interpolation join
      case (_,
      me1 @ MetaEntry(_, MetaDimension(_, _, DimensionSpace.CONTINUOUS), units1),
      MetaEntry(_, MetaDimension(_, _, DimensionSpace.CONTINUOUS), units2))
        if units1.unitsTag.domainType == DomainType.POINT && units2.unitsTag.domainType == DomainType.RANGE => {
        InterpolationJoin(dsID2, ExplodeList(dsID1, Seq(dsID1.metaSource.columnForEntry(me1)).flatten), 1000 /* WINDOW SIZE ??? */ ).asOption
      }

      // Can't join
      case _ => None
    }
  }
}
