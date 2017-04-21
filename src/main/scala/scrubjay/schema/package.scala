package scrubjay


import org.apache.spark.sql.catalyst.parser.LegacyTypeStringParser
import org.apache.spark.sql.types.{DataType, StructType}
import scrubjay.metabase._

import scala.io.Source
import scala.util.Try

package object schema {

  def fromJSON(json: String): StructType = {
    Try(DataType.fromJson(json)).getOrElse(LegacyTypeStringParser.parse(json)) match {
      case t: StructType => t
      case _ => throw new RuntimeException(s"Failed parsing StructType: $json")
    }
  }

  def fromJSONFile(filename: String): StructType = {
    fromJSON(Source.fromFile(filename).getLines().mkString("\n"))
  }
  /*

  type MetaSource = Map[String, MetaEntry]

  def haveSharedProperty(me1: MetaEntry, me2: MetaEntry, property: (MetaEntry) => Boolean): Boolean = {
    property(me1) && property(me2)
  }

  implicit class MetaSourceImplicits(metaSource: MetaSource) extends Serializable {

    val columns: Seq[String] = metaSource.keys.toSeq

    def columnForEntry(me: MetaEntry): Option[String] = {
      metaSource.map(_.swap).get(me)
    }

    def filterEntries(cond: MetaEntry => Boolean): MetaSource = {
      metaSource.filter { case (_, me) => cond(me) }
    }

    def withMetaEntries(newMetaEntryMap: MetaSource, overwrite: Boolean = false): MetaSource = {
      if (overwrite) {
        metaSource ++ newMetaEntryMap
      }
      else {
        val newEntries = newMetaEntryMap.filterNot(entry => metaSource.keySet.contains(entry._1))
        metaSource ++ newEntries
      }
    }

    def withColumns(newColumns: Seq[String], overwrite: Boolean = false): MetaSource = {
      if (overwrite) {
        metaSource ++ newColumns.map(_ -> UNKNOWN_META_ENTRY)
      }
      else {
        val knownEntries = metaSource.filter { case (k, _) => newColumns.contains(k) }
        val newEntries = newColumns.filterNot(metaSource.keySet.contains)
        knownEntries ++ newEntries.map(_ -> UNKNOWN_META_ENTRY)
      }
    }

    def withoutColumns(oldColumns: Seq[String]): MetaSource = {
      metaSource.filterNot { case (k, _) => oldColumns.contains(k) }
    }

    def saveToCSV(fileName: String): Unit = {

      import java.io.{BufferedWriter, FileWriter}
      import scrubjay.metabase.MetaDescriptor.MetaRelationType

      val bw = new BufferedWriter(new FileWriter(fileName))

      bw.write("column, relationType, meaning, dimension, units")
      bw.newLine()

      metaSource.foreach{case (column, metaEntry) =>
        val rowString = Seq(
          column,
          MetaRelationType.toString(metaEntry.relationType),
          metaEntry.dimension.title,
          metaEntry.units.title
        ).mkString(",")

        bw.write(rowString)
        bw.newLine()
      }

      bw.close()
    }

  }
  */
}
