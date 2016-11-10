/**
 * Single entry point for ScrubJay API
 *
 * All functionality here is meant to be externally accessible via `import scrubjay.imports._`
 *
 */

import scrubjay.datasource._
import scrubjay.metabase._
import scrubjay.metasource._
import scrubjay.query._
import com.datastax.spark.connector._
import org.apache.spark._
import org.joda.time.Period

package object scrubjay {

  /**
   * Types
   */

  type DataSource = scrubjay.datasource.DataSource
  type MetaSource = metasource.MetaSource
  type MetaEntry = scrubjay.metabase.MetaEntry
  type Query = scrubjay.query.Query

  /**
   * Standalone functions
   */

  def metaEntryFromStrings = scrubjay.metabase.MetaEntry.metaEntryFromStrings _

  def createCSVMetaSource = scrubjay.metasource.CSVMetaSource.createCSVMetaSource _
  def createLocalMetaSource = scrubjay.metasource.LocalMetaSource.createLocalMetaSource _
  def createCassandraMetaSource = scrubjay.metasource.CassandraMetaSource.createCassandraMetaSource _


  /**
   * SparkContext implicit functions
   */

  implicit class ScrubJaySessionImplicits(sc: SparkContext) {

    val metaBase = GlobalMetaBase.META_BASE

    /**
     * Create a LocalDataSource from a sequence of RawDataRow, i.e. Map[String -> Any]
     */

    def createLocalDataSource(rawData: Seq[RawDataRow],
                              columns: Seq[String],
                              metaSource: MetaSource = MetaSource.empty): Option[LocalDataSource] = {
      scrubjay.datasource.LocalDataSource.createLocalDataSource(sc.parallelize(rawData), columns, metaSource)
    }

    /**
     * Create a CassandraDataSource from an existing Cassandra table
     */

    def createCassandraDataSource(keyspace: String,
                                  table: String,
                                  metaSource: MetaSource = MetaSource.empty,
                                  selectColumns: Seq[String] = Seq.empty,
                                  whereConditions: Seq[String] = Seq.empty,
                                  limit: Option[Long] = None): Option[CassandraDataSource] = {
      scrubjay.datasource.CassandraDataSource.createCassandraDataSource(sc.cassandraTable(keyspace, table), metaSource, selectColumns, whereConditions, limit)
    }

    /**
     * Create a CSVDataSource from a CSV file with a header
     */

    def createCSVDataSource(csvFileName: String,
                            metaSource: MetaSource = MetaSource.empty): Option[CSVDataSource] = {

      // Unfortunately, this code needs to be here to avoid passing the SparkContext to any
      // underlying function that may be serialized
      import au.com.bytecode.opencsv.CSVReader
      import scala.collection.JavaConversions._
      import java.io._

      val reader = new CSVReader(new FileReader(csvFileName))
      val header = reader.readNext.map(_.trim)
      val rawData = reader.readAll.map(row => header.zip(row.map(_.trim)).toMap).toList

      scrubjay.datasource.CSVDataSource.createCSVDataSource(sc.parallelize(rawData), header, csvFileName, metaSource)
    }

    def runQuery(dataSources: Set[DataSource], metaEntries: Set[MetaEntry]): Iterator[DataSource] = {
      new Query(dataSources, metaEntries).run
    }
  }

  /**
   * DataSource implicit functions
   */

  implicit class DataSourceImplicits(ds: DataSource) {

    /**
     * Derivations
     */

    def deriveTimeSpan = {
      new scrubjay.derivation.TimeSpan(Some(ds)).apply
    }

    def deriveExplodeList(columns: Seq[String]) = {
      new scrubjay.derivation.ExplodeList(Some(ds), columns).apply
    }

    def deriveExplodeTimeSpan(columnsWithPeriods: Seq[(String, Period)]) = {
      new scrubjay.derivation.ExplodeTimeSpan(Some(ds), columnsWithPeriods).apply
    }

    def deriveNaturalJoin(ds2: Option[DataSource]) = {
      new scrubjay.derivation.NaturalJoin(Some(ds), ds2).apply
    }

    def deriveInterpolationJoin(ds2: Option[DataSource], window: Double) = {
      new scrubjay.derivation.InterpolationJoin(Some(ds), ds2, window).apply
    }

    def deriveRangeJoin(ds2: Option[DataSource]) = {
      new scrubjay.derivation.RangeJoin(Some(ds), ds2).apply
    }

    /**
     * Save formats
     */

    def saveToCassandra(keyspace: String, table: String) = {
      scrubjay.datasource.CassandraDataSource.saveToCassandra(ds, keyspace, table)
    }

    def createCassandraTable(keyspace: String, table: String, primaryKeys: Seq[String], clusterKeys: Seq[String]) = {
      scrubjay.datasource.CassandraDataSource.createCassandraTable(ds, keyspace, table, primaryKeys, clusterKeys)
    }

    def saveToCSV(filename: String,
                  wrapperChar: String = "\"",
                  delimiter: String = ",",
                  noneString: String = "null") = {
      scrubjay.datasource.CSVDataSource.saveToCSV(ds, filename, wrapperChar, delimiter, noneString)
    }

    /**
      * Create a new LocalMetaSource by interactively choosing them from the GlobalMetaBase
      */
    def createMetaSourceInteractively: MetaSource = {

      createLocalMetaSource {
        {
          for (column <- ds.metaSource.columns) yield {

            // Get meaning
            GlobalMetaBase.META_BASE.meaningBase.foreach(meaning =>
              println(String.format("%-30s %s", meaning._1, meaning._2.description))
            )
            val meaning = scala.io.StdIn.readLine("Specify a meaning for column " + column + ": ")

            // Get dimension
            GlobalMetaBase.META_BASE.dimensionBase.foreach(dimension =>
              println(String.format("%-30s %s", dimension._1, dimension._2.description))
            )
            val dimension = scala.io.StdIn.readLine("Specify a dimension for column " + column + ": ")

            // Get units
            GlobalMetaBase.META_BASE.unitsBase.foreach(units =>
              println(String.format("%-30s %s", units._1, units._2.description))
            )
            val units = scala.io.StdIn.readLine("Specify units for column " + column + ": ")

            column -> metaEntryFromStrings(meaning, dimension, units)
          }
        }.toMap
      }
    }
  }

  /**
   * MetaSource implicit functions
   */

  implicit class MetaSourceImplicits(metaSource: MetaSource) {

    /**
      * Save formats
      */

    def saveToCSV(fileName: String) = {
      scrubjay.metasource.CSVMetaSource.saveToCSV(metaSource, fileName)
    }

    def saveToCassandra(sc: SparkContext, keyspace: String, table: String) = {
      scrubjay.metasource.CassandraMetaSource.saveToCassandra(metaSource, sc, keyspace, table)
    }

  }

}
