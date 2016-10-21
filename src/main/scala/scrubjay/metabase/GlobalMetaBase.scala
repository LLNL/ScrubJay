package scrubjay.metabase

import scrubjay.units._

import scrubjay.metabase.MetaDescriptor._

object GlobalMetaBase {

  final val META_BASE: MetaBase = new MetaBase

  // Unknowns - REQUIRED
  final val DIMENSION_UNKNOWN = META_BASE.addDimension(MetaDimension("unknown", "The upside down", DimensionType.UNKNOWN))
  final val MEANING_UNKNOWN = META_BASE.addMeaning(MetaMeaning("unknown", "Life"))
  final val UNITS_UNKNOWN = META_BASE.addUnits(MetaUnits("unknown", "Squeebles", Identifier))

  // Meanings
  final val MEANING_IDENTITY = META_BASE.addMeaning(MetaMeaning("identity", "A single identity"))
  final val MEANING_START = META_BASE.addMeaning(MetaMeaning("start", "The beginning of something"))
  final val MEANING_END = META_BASE.addMeaning(MetaMeaning("end", "The end of something"))
  final val MEANING_DURATION = META_BASE.addMeaning(MetaMeaning("duration", "A quantity of time"))
  final val MEANING_SPAN = META_BASE.addMeaning(MetaMeaning("duration", "A span of time"))
  final val MEANING_CUMULATIVE = META_BASE.addMeaning(MetaMeaning("cumulative", "An atomically increasing value representing some thing since some point of origin"))

  // Dimensions
  final val DIMENSION_TIME = META_BASE.addDimension(MetaDimension("time", "The time dimension", DimensionType.CONTINUOUS))
  final val DIMENSION_TEMPERATURE = META_BASE.addDimension(MetaDimension("temperature", "The temperature dimension", DimensionType.CONTINUOUS))
  final val DIMENSION_NODE = META_BASE.addDimension(MetaDimension("node", "A single node in an HPC cluster", DimensionType.DISCRETE))
  final val DIMENSION_RACK = META_BASE.addDimension(MetaDimension("rack", "A rack (containing nodes) in an HPC cluster", DimensionType.DISCRETE))
  final val DIMENSION_FLOPS = META_BASE.addDimension(MetaDimension("flops", "Floating-point operations", DimensionType.CONTINUOUS))

  // Units
  // *******************************************************************************
  // *
  // * To add new units to the knowledge base:
  // *
  // *   1. Add a new class that extends Units and implements `value`
  // *   2. Add a companion object for that class that extends UnitsTag and implements `rawValueClass` and `convert`
  // *   3. Add a new entry to the GlobalMetaBase below
  // *
  // *   Example:
  // *
  // *      // units/SomeUnits.scala
  // *      case class SomeUnits(value: Double) extends Units[Double]
  // *
  // *      object SomeUnits extends UnitsTag[SomeUnits] {
  // *        val rawValueClass = classTag[Double] // must be same as type of `value`
  // *        def convert(value: Any, metaUnits: MetaUnits) = new SomeUnits(value.toDouble)
  // *      }
  // *
  // *
  // *      // units/GlobalMetaBase.scala
  // *      final val UNITS_SOME_UNITS = META_BASE.addUnits(MetaUnits("SomeUnits", "Some description", SomeUnits))
  // *
  // *******************************************************************************
  final val UNITS_IDENTIFIER = META_BASE.addUnits(MetaUnits("identifier", "A categorical identifier", Identifier))
  final val UNITS_SECONDS = META_BASE.addUnits(MetaUnits("seconds", "A quantity of seconds", Seconds))
  final val UNITS_DEGREES_CELSIUS = META_BASE.addUnits(MetaUnits("degrees Celsius", "A measured temperature in degrees Celsius", DegreesCelsius))
  final val UNITS_DATETIMESTAMP = META_BASE.addUnits(MetaUnits("datetimestamp", "An instant in time, by date and time", DateTimeStamp))
  final val UNITS_DATETIMESPAN = META_BASE.addUnits(MetaUnits("datetimespan", "A span of time, by date and time, with a start and and end", DateTimeSpan))
  final val UNITS_COUNT = META_BASE.addUnits(MetaUnits("count", "A discrete, positive quantity (whole numbers)", Count))


  // Composite Units
  final val UNITS_COMPOSITE_LIST = META_BASE.addUnits(MetaUnits("list", "A list of...", UnitsList))
}
