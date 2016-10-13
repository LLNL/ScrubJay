package scrubjay.metasource

import java.io.{BufferedWriter, FileReader, FileWriter}

import au.com.bytecode.opencsv.CSVReader
import scrubjay.metabase.MetaEntry

import scala.collection.JavaConversions._

object CSVMetaSource {

  /*
   *  A CSVMetaSource file looks like:
   *
   *  column, meaning, dimension, units
   *  "START_TIME", "start", "time", "datetimestamp"
   *  "TEMP", "temperature", "temperature", "degrees fahrenheit"
   *
   */

  def createCSVMetaSource(filename: String): MetaSource = {
    val reader = new CSVReader(new FileReader(filename))
    val header = reader.readNext.map(_.trim)
    val data = reader.readAll.map(row => header.zip(row.map(_.trim)).toMap)
    val metaEntryMap = data.map(row =>
      (row("column"), MetaEntry.metaEntryFromStrings(row("meaning"), row("dimension"), row("units")))).toMap

    new MetaSource(metaEntryMap)
  }

  def saveToCSV(m: MetaSource, fileName: String): Unit = {

    val bw = new BufferedWriter(new FileWriter(fileName))

    bw.write("column, meaning, dimension, units")
    bw.newLine()

    m.metaEntryMap.foreach{case (column, metaEntry) => {
      val rowString = Seq(column, metaEntry.meaning.title, metaEntry.dimension.title, metaEntry.units.title).mkString(",")
      bw.write(rowString)
      bw.newLine()
    }}
  }
}
