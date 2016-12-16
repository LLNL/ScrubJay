package scrubjay.metabase

import scrubjay.units._

import scrubjay.metabase.MetaDescriptor._

object GlobalMetaBase {

  final val META_BASE: MetaBase = new MetaBase

  // Unknowns - REQUIRED
  final val DIMENSION_UNKNOWN: MetaDimension = META_BASE.addDimension(MetaDimension("unknown", "The upside down", DimensionSpace.DISCRETE))
  final val MEANING_UNKNOWN: MetaMeaning = META_BASE.addMeaning(MetaMeaning("unknown", "Life"))
  final val UNITS_UNKNOWN: MetaUnits = META_BASE.addUnits(MetaUnits("unknown", "Squeebles", UnorderedDiscrete))

  // Meanings
  final val MEANING_IDENTITY: MetaMeaning = META_BASE.addMeaning(MetaMeaning("identity", "A single identity"))
  final val MEANING_START: MetaMeaning = META_BASE.addMeaning(MetaMeaning("start", "The beginning of something"))
  final val MEANING_END: MetaMeaning = META_BASE.addMeaning(MetaMeaning("end", "The end of something"))
  final val MEANING_DURATION: MetaMeaning = META_BASE.addMeaning(MetaMeaning("duration", "A quantity of time"))
  final val MEANING_SPAN: MetaMeaning = META_BASE.addMeaning(MetaMeaning("span", "A span of time"))
  final val MEANING_CUMULATIVE: MetaMeaning = META_BASE.addMeaning(MetaMeaning("cumulative", "An atomically increasing value representing some thing since some point of origin"))

  // Dimensions
  final val DIMENSION_TIME: MetaDimension = META_BASE.addDimension(MetaDimension("time", "The time dimension", DimensionSpace.CONTINUOUS))
  final val DIMENSION_TEMPERATURE: MetaDimension = META_BASE.addDimension(MetaDimension("temperature", "The temperature dimension", DimensionSpace.CONTINUOUS))
  final val DIMENSION_NODE: MetaDimension = META_BASE.addDimension(MetaDimension("node", "A single node in an HPC cluster", DimensionSpace.DISCRETE))
  final val DIMENSION_RACK: MetaDimension = META_BASE.addDimension(MetaDimension("rack", "A rack (containing nodes) in an HPC cluster", DimensionSpace.DISCRETE))
  final val DIMENSION_FLOPS: MetaDimension = META_BASE.addDimension(MetaDimension("flops", "Floating-point operations", DimensionSpace.CONTINUOUS))

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
  final val UNITS_UNORDERED_DISCRETE: MetaUnits = META_BASE.addUnits(MetaUnits("identifier", "A categorical identifier", UnorderedDiscrete))
  final val UNITS_ORDERED_DISCRETE: MetaUnits = META_BASE.addUnits(MetaUnits("count", "A discrete, positive quantity (whole numbers)", OrderedDiscrete))
  final val UNITS_ORDERED_CONTINUOUS: MetaUnits = META_BASE.addUnits(MetaUnits("amount", "An amount that is ordered and continuous (real numbers)", OrderedContinuous))

  final val UNITS_SECONDS: MetaUnits = META_BASE.addUnits(MetaUnits("seconds", "A quantity of seconds", Seconds))
  final val UNITS_DEGREES_CELSIUS: MetaUnits = META_BASE.addUnits(MetaUnits("degrees Celsius", "A measured temperature in degrees Celsius", DegreesCelsius))
  final val UNITS_DATETIMESTAMP: MetaUnits = META_BASE.addUnits(MetaUnits("datetimestamp", "An instant in time, by date and time", DateTimeStamp))
  final val UNITS_DATETIMESPAN: MetaUnits = META_BASE.addUnits(MetaUnits("datetimespan", "A span of time, by date and time, with a start and and end", DateTimeSpan))


  // Composite Units
  final val UNITS_COMPOSITE_LIST: MetaUnits = META_BASE.addUnits(MetaUnits("list", "A list of...", UnitsList))
}
