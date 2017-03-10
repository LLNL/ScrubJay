package scrubjay.transformation

import scrubjay.datasource._
import scrubjay.metabase.MetaDescriptor.DimensionSpace
import scrubjay.units.UnitsTag.DomainType


object UberExplode {

  def apply(dsID: DataSourceID): Seq[DataSourceID] = {

    val explodeColumns = dsID.metaSource.filter(e =>
      e._2.units.unitsTag.domainType == DomainType.MULTIPOINT ||
      e._2.units.unitsTag.domainType == DomainType.RANGE)
      .keys.toSeq

    // Every length of combinations
    1.to(explodeColumns.length).flatMap(i => {

      // Every combination
      explodeColumns.combinations(i).map(columns => {

        // Explode all
        columns.foldLeft(dsID)((dsID, column) => {
          val metaEntry = dsID.metaSource(column)
          val space = metaEntry.dimension.dimensionType

          // Determine explode function
          space match {
            case DimensionSpace.DISCRETE => ExplodeDiscreteRange(dsID, column)
            case DimensionSpace.CONTINUOUS => ExplodeContinuousRange(dsID, column, 60000) // FIXME: explode period
            case _ => dsID
          }
        })
      })
    })
  }
}
