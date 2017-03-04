package scrubjay.metasource

import java.io.FileReader

import au.com.bytecode.opencsv.CSVReader
import scrubjay.metabase.MetaEntry

import scala.collection.JavaConversions._

/*
 *  A CSVMetaSource file looks like:
 *
 *  column, meaning, dimension, units
 *  "START_TIME", "start", "time", "datetimestamp"
 *  "TEMP", "temperature", "temperature", "degrees fahrenheit"
 *
 */

case class CSVMetaSource(filename: String) extends MetaSourceID {

  def realize: MetaSource = {
    val reader = new CSVReader (new FileReader (filename) )
    val header = reader.readNext.map (_.trim)
    val data = reader.readAll.map (row => header.zip (row.map (_.trim) ).toMap)
    val metaSource = data.map (row =>
      (row ("column"), MetaEntry.metaEntryFromStrings (row ("relationType"), row ("meaning"), row ("dimension"), row ("units") ) ) ).toMap

    metaSource
  }

}
