package scrubjay.meta

import scrubjay.units._

import scala.reflect._

object GlobalMetaBase {

  final val META_BASE: MetaBase = new MetaBase

  // Unknowns - REQUIRED
  final val DIMENSION_UNKNOWN = META_BASE.addDimension(MetaDimension("unknown", "The upside down"))
  final val MEANING_UNKNOWN = META_BASE.addMeaning(MetaMeaning("unknown", "Life"))
  final val UNITS_UNKNOWN = META_BASE.addUnits(MetaUnits("unknown", "Squeebles", classTag[Identifier]))

  // Meanings
  final val MEANING_IDENTITY = META_BASE.addMeaning(MetaMeaning("identity", "A single identity"))
  final val MEANING_START = META_BASE.addMeaning(MetaMeaning("start", "The beginning of something"))
  final val MEANING_END = META_BASE.addMeaning(MetaMeaning("end", "The end of something"))
  final val MEANING_DURATION = META_BASE.addMeaning(MetaMeaning("duration", "A quantity of time"))
  final val MEANING_SPAN = META_BASE.addMeaning(MetaMeaning("duration", "A span of time"))
  final val MEANING_CUMULATIVE = META_BASE.addMeaning(MetaMeaning("cumulative", "An atomically increasing value representing some thing since some point of origin"))

  // Dimensions
  final val DIMENSION_TIME = META_BASE.addDimension(MetaDimension("time", "The time dimension"))
  final val DIMENSION_NODE = META_BASE.addDimension(MetaDimension("node", "A single node in an HPC cluster"))
  final val DIMENSION_RACK = META_BASE.addDimension(MetaDimension("rack", "A rack (containing nodes) in an HPC cluster"))
  final val DIMENSION_FLOPS = META_BASE.addDimension(MetaDimension("flops", "Floating-point operations"))

  // Units
  // *******************************************************************************
  // ** IMPORTANT: see units/package.scala for information on creating new units! **
  // *******************************************************************************
  final val UNITS_IDENTIFIER = META_BASE.addUnits(MetaUnits("identifier", "A categorical identifier", classTag[Identifier]))
  final val UNITS_SECONDS = META_BASE.addUnits(MetaUnits("seconds", "A categorical identifier", classTag[Seconds]))
  final val UNITS_DATETIMESTAMP = META_BASE.addUnits(MetaUnits("datetimestamp", "An instant in time, by date and time", classTag[DateTimeStamp]))
  final val UNITS_DATETIMESPAN = META_BASE.addUnits(MetaUnits("datetimespan", "A span of time, by date and time, with a start and and end", classTag[DateTimeSpan]))
  final val UNITS_COUNT = META_BASE.addUnits(MetaUnits("count", "A discrete, positive quantity (whole numbers)", classTag[Count]))


  // Composite Units
  final val UNITS_COMPOSITE_LIST = META_BASE.addUnits(MetaUnits("list", "A list of...", classTag[UnitsList[_]]))
}
