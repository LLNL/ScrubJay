package scrubjay

import scala.reflect._
import scala.reflect.ClassTag

/*
 *
 * --- Creating new Units ---
 *
 * To add new units to the knowledge base:
 *   1. Add a new class, e.g. SomeUnits that extends Units, in this directory, e.g. units/SomeUnits.scala
 *      - Must implement `val raw = v` , the raw representation that can be converted to a SomeUnits instance
 *      - Must include an accompanying SomeUnits object with a UnitsConverter[SomeUnits] that uses an Any
 *        value and its associated MetaDescriptor to construct a new instance of SomeUnits
 *      - Your converter may throw a RuntimeException if given an invalid value to convert
 *   2. Add a new entry to converterForClassTag in units/package.scala
 *      - e.g. classTag[SomeUnits] -> SomeUnits.converter
 *   3. Add a new MetaUnits entry to the GlobalMetaBase in meta/GlobalMetaBase.scala
 *
 */


package object units {

  var converterForClassTag: Map[ClassTag[_], UnitsConverter[_]] = Map(
    classTag[Identifier] -> Identifier.converter,
    classTag[UnitsList[_]] -> UnitsList.converter,
    classTag[Seconds] -> Seconds.converter,
    classTag[Count] -> Count.converter,
    classTag[DateTimeStamp] -> DateTimeStamp.converter,
    classTag[DateTimeSpan] -> DateTimeSpan.converter
  )

}
