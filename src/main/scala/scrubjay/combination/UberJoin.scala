package scrubjay.combination

import scrubjay.dataset.DatasetID
import scrubjay.metabase.MetaDescriptor._
import scrubjay.schema._
//import scrubjay.transformation.{ExplodeContinuousRange, ExplodeDiscreteRange}

/*
object UberJoin {

  // FIXME: (throughout) window size, period length
  val interJoinWindow = 60000
  val explodePeriod = 60000

  def UberInterpolationJoin(dsID1: DatasetID,
                            dsID2: DatasetID,
                            commonContinuousDomainDimensions: Seq[(MetaDimension, MetaEntry, MetaEntry)]): Seq[DatasetID] = {

    commonContinuousDomainDimensions.flatMap{

      // Point, Point => interpolationjoin(ds1, ds2)
      case (dim, me1, me2) if haveSharedProperty(me1, me2, _.units.unitsTag.domainType == DomainType.POINT) => {
        val dsID1Join = dsID1
        val dsID2Join = dsID2
        Seq(
          InterpolationJoin(dsID1Join, dsID2Join, interJoinWindow),
          InterpolationJoin(dsID2Join, dsID1Join, interJoinWindow)
        )
      }

      // Point, Range => interpolationjoin(ds1, explode(ds2))
      case (dim, me1, me2) if me1.units.unitsTag.domainType == DomainType.POINT && me2.units.unitsTag.domainType == DomainType.RANGE => {
        val dsID1Join = dsID1
        val dsID2Join = ExplodeContinuousRange(dsID2, dsID2.metaSource.columnForEntry(me2).get, explodePeriod)
        Seq(
          InterpolationJoin(dsID1Join, dsID2Join, explodePeriod),
          InterpolationJoin(dsID2Join, dsID1Join, explodePeriod))
      }

      // Range, Point => interpolationjoin(explode(ds1), ds2)
      case (dim, me1, me2) if me1.units.unitsTag.domainType == DomainType.RANGE && me2.units.unitsTag.domainType == DomainType.POINT => {
        val dsID1Join = ExplodeContinuousRange(dsID1, dsID1.metaSource.columnForEntry(me1).get, explodePeriod)
        val dsID2Join = dsID2
        Seq(
          InterpolationJoin(dsID1Join, dsID2Join, interJoinWindow),
          InterpolationJoin(dsID2Join, dsID1Join, interJoinWindow)
        )
      }

      // no other cases for now
      case _ => Seq()

      // TODO: add MULTIPOINT case
    }
  }

  def UberNaturalJoin(dsID1: DatasetID,
                      dsID2: DatasetID,
                      commonDiscreteDomainDimensions: Seq[(MetaDimension, MetaEntry, MetaEntry)]): Seq[DatasetID] = {

    commonDiscreteDomainDimensions.flatMap{

      // Point, Point => naturaljoin(ds1, ds2)
      case (dim, me1, me2) if haveSharedProperty(me1, me2, _.units.unitsTag.domainType == DomainType.POINT) => {
        val dsID1Join = dsID1
        val dsID2Join = dsID2
        Seq(NaturalJoin(dsID1, dsID2), NaturalJoin(dsID1Join, dsID2Join))
      }

      // Point, Range => naturaljoin(ds1, explode(ds2))
      case (dim, me1, me2) if me1.units.unitsTag.domainType == DomainType.POINT && me2.units.unitsTag.domainType == DomainType.MULTIPOINT => {
        val dsID1Join = dsID1
        val dsID2Join = ExplodeDiscreteRange(dsID2, dsID2.metaSource.columnForEntry(me2).get)
        Seq(NaturalJoin(dsID1Join, dsID2Join), NaturalJoin(dsID2Join, dsID1Join))
      }

      // Range, Point => interpolationjoin(explode(ds1), ds2)
      case (dim, me1, me2) if me1.units.unitsTag.domainType == DomainType.MULTIPOINT && me2.units.unitsTag.domainType == DomainType.POINT => {
        val dsID1Join = ExplodeDiscreteRange(dsID1, dsID1.metaSource.columnForEntry(me1).get)
        val dsID2Join = dsID2
        Seq(NaturalJoin(dsID1, dsID2))
      }

      // no other cases for now
      case _ => Seq()
    }
  }

  // Get shared domain dimensions
  def apply(dsID1: DatasetID, dsID2: DatasetID): Seq[DatasetID] = {

    val commonDomainDimensions = MetaSource.commonDimensionEntries(dsID1.metaSource, dsID2.metaSource)
      .filter(e => haveSharedProperty(e._2, e._3, _.relationType == MetaRelationType.DOMAIN))

    val commonContinuousDomainDimensions = commonDomainDimensions
      .filter(e => haveSharedProperty(e._2, e._3, _.dimension.dimensionType == DimensionSpace.CONTINUOUS))

    // If any continuous domain exists, we HAVE to use it
    if (commonContinuousDomainDimensions.nonEmpty)
      Seq(
        InterpolationJoin(dsID1, dsID2, interJoinWindow).asOption,
        InterpolationJoin(dsID2, dsID1, interJoinWindow).asOption
      ).flatten
    else
      Seq(
        NaturalJoin(dsID1, dsID2).asOption,
        NaturalJoin(dsID2, dsID1).asOption
      ).flatten
  }
}
*/
