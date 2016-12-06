package scrubjay.metasource

import scrubjay.metabase._

import org.apache.spark.SparkContext
import com.datastax.spark.connector._

object CassandraMetaSource {

  /*
   *  A CassandraMetaSource file looks like:
   *
   *  column, meaning, dimension, units
   *  "START_TIME", "start", "time", "datetimestamp"
   *  "TEMP", "temperature", "temperature", "degrees fahrenheit"
   *
   */

  def createCassandraMetaSource(sc: SparkContext, keyspace: String, table: String): MetaSource = {
    val data = sc.cassandraTable(keyspace, table).map(_.toMap.map{case (k, v) => (k, v.toString)}.toMap).collect
    val metaEntryMap = data.map(row =>
      (row("column"), MetaEntry.metaEntryFromStrings(row("relationType"), row("meaning"), row("dimension"), row("units")))).toMap

    new MetaSource(metaEntryMap)
  }

  def saveToCassandra(m: MetaSource, sc: SparkContext, keyspace: String, table: String): Unit = {
    val cassandraRows = m.metaEntryMap.map{case (column, metaEntry) => {
      CassandraRow.fromMap(Map(
        "column" -> column,
        "meaning" -> metaEntry.meaning.title,
        "dimension" -> metaEntry.dimension.title,
        "units" -> metaEntry.units.title
      ))
    }}.toSeq
    sc.parallelize(cassandraRows).saveToCassandra(keyspace, table)
  }
}

