package scrubjay.metabase

import scrubjay.metabase.MetaDescriptor._


class MetaBase(db: Map[String, MetaDimension] = Map.empty,
               ub: Map[String, MetaUnits] = Map.empty) extends Serializable {

  var dimensionBase: Map[String, MetaDimension] = db
  var unitsBase: Map[String, MetaUnits] = ub

  def addDimension(m: MetaDimension): MetaDimension = {
    dimensionBase ++= Map(m.title -> m)
    m
  }

  def addUnits(m: MetaUnits): MetaUnits = {
    unitsBase ++= Map(m.title -> m)
    m
  }
}
