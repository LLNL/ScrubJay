package scrubjay.meta

import scrubjay.units._

import scala.reflect._

object GlobalMetaBase {

  final val META_BASE: MetaBase = new MetaBase

  // Unknowns
  final val DIMENSION_UNKNOWN = META_BASE.addDimension(MetaDimension("unknown", "The upside down"))
  final val MEANING_UNKNOWN = META_BASE.addMeaning(MetaMeaning("unknown", "Life"))
  final val UNITS_UNKNOWN = META_BASE.addUnits(MetaUnits("unknown", "Squeebles", classTag[Identifier]))

  // Meanings
  final val MEANING_IDENTITY = META_BASE.addMeaning(MetaMeaning("identity", "A single identity"))
  final val MEANING_START = META_BASE.addMeaning(MetaMeaning("start", "The beginning of something"))
  final val MEANING_DURATION = META_BASE.addMeaning(MetaMeaning("duration", "A span of time"))

  // Dimensions
  final val DIMENSION_TIME = META_BASE.addDimension(MetaDimension("time", "The time dimension"))
  final val DIMENSION_NODE = META_BASE.addDimension(MetaDimension("node", "A single node in an HPC cluster"))
  final val DIMENSION_RACK = META_BASE.addDimension(MetaDimension("rack", "A rack (containing nodes) in an HPC cluster"))

  // Units
  final val UNITS_IDENTIFIER = META_BASE.addUnits(MetaUnits("identifier", "A categorical identifier", classTag[Identifier]))
  final val UNITS_SECONDS = META_BASE.addUnits(MetaUnits("seconds", "Quantity of seconds", classTag[Seconds]))

  // Composite Units
  final val UNITS_COMPOSITE_LIST = META_BASE.addUnits(MetaUnits("list", "A list of...", classTag[UnitsList[_]]))
}
