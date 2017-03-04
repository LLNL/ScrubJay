package scrubjay.metasource

import scrubjay.metabase._
import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import scrubjay.metabase.MetaDescriptor.MetaRelationType


/*
 *  A CassandraMetaSource file looks like:
 *
 *  column, meaning, dimension, units
 *  "START_TIME", "start", "time", "datetimestamp"
 *  "TEMP", "temperature", "temperature", "degrees fahrenheit"
 *
 */

case class CassandraMetaSource(keyspace: String, table: String) extends MetaSourceID {
  def realize: MetaSource = {
    val sc = SparkContext.getOrCreate()
    val data = sc.cassandraTable(keyspace, table).map(_.toMap.map{case (k, v) => (k, v.toString)}.toMap).collect
    val metaSource = data.map(row =>
      (row("column"), MetaEntry.metaEntryFromStrings(row("relationType"), row("meaning"), row("dimension"), row("units")))).toMap

    metaSource
  }
}

object CassandraMetaSource {

  def saveToCassandra(m: MetaSource, keyspace: String, table: String): Unit = {
    val cassandraRows = m.map{case (column, metaEntry) =>
      CassandraRow.fromMap(Map(
        "column" -> column,
        "relationType" -> MetaRelationType.toString(metaEntry.relationType),
        "meaning" -> metaEntry.meaning.title,
        "dimension" -> metaEntry.dimension.title,
        "units" -> metaEntry.units.title
      ))
    }.toSeq
    SparkContext.getOrCreate().parallelize(cassandraRows).saveToCassandra(keyspace, table)
  }
}

