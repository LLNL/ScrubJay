package scrubjay.metabase

import scrubjay.metabase.MetaDescriptor._


class MetaBase(mb: Map[String, MetaMeaning] = Map.empty,
               db: Map[String, MetaDimension] = Map.empty,
               ub: Map[String, MetaUnits] = Map.empty) extends Serializable {

  var meaningBase: Map[String, MetaMeaning] = mb
  var dimensionBase: Map[String, MetaDimension] = db
  var unitsBase: Map[String, MetaUnits] = ub

  def addMeaning(m: MetaMeaning): MetaMeaning = {
    meaningBase ++= Map(m.title -> m)
    m
  }

  def addDimension(m: MetaDimension): MetaDimension = {
    dimensionBase ++= Map(m.title -> m)
    m
  }

  def addUnits(m: MetaUnits): MetaUnits = {
    unitsBase ++= Map(m.title -> m)
    m
  }
}
