/*
 * Single entry point for ScrubJay API
 *
 * All functions here are meant to be externally accessible via `import scrubjay.imports._`
 *
 */

import org.apache.spark._
import scrubjay.datasource.{CSVDataSource, CassandraDataSource, LocalDataSource, RawDataRow}
import scrubjay.meta.GlobalMetaBase

package object scrubjay {

  /*
   * Types
   */

  type MetaSource = scrubjay.meta.MetaSource
  type EmptyMetaSource = scrubjay.meta.EmptyMetaSource
  type MetaEntry = scrubjay.meta.MetaEntry

  type DataSource = scrubjay.datasource.DataSource

  type NaturalJoin = scrubjay.derivation.NaturalJoin
  type DeriveTimeSpan = scrubjay.derivation.DeriveTimeSpan
  type DeriveExplodeList = scrubjay.derivation.ExplodeList

  type Query = scrubjay.query.Query

  /*
   * Standalone functions
   */

  def metaEntryFromStrings = scrubjay.meta.MetaEntry.metaEntryFromStrings _

  def createCSVMetaSource = scrubjay.metasource.CSVMetaSource.createCSVMetaSource _
  def createLocalMetaSource = scrubjay.metasource.LocalMetaSource.createLocalMetaSource _


  /*
   * SparkContext implicit functions
   */

  implicit class ScrubJaySessionImplicits(sc: SparkContext) {
    sc.setLogLevel("WARN")
    val metaBase = GlobalMetaBase.META_BASE

    def createLocalDataSource(rawData: Seq[RawDataRow],
                              columns: Seq[String],
                              metaSource: MetaSource): LocalDataSource = {
      new LocalDataSource(sc.parallelize(rawData), columns, metaSource)
    }

    def createCassandraDataSource(keyspace: String,
                                  table: String,
                                  metaSource: MetaSource = new EmptyMetaSource,
                                  selectColumns: Seq[String] = Seq.empty,
                                  whereConditions: Seq[String] = Seq.empty): CassandraDataSource = {

      import com.datastax.spark.connector._

      val cassandraRdd = {
        val cassRdd = sc.cassandraTable(keyspace, table)
        val cassRddSelected = selectColumns.foldLeft(cassRdd)(_.select(_))
        val cassRddSelectWhere = whereConditions.foldLeft(cassRddSelected)(_.where(_))
        cassRddSelectWhere
      }

      new CassandraDataSource(cassandraRdd, metaSource)
    }

    def createCSVDataSource(filename: String,
                            metaSource: MetaSource = new EmptyMetaSource): CSVDataSource = {

      import au.com.bytecode.opencsv.CSVReader
      import scala.collection.JavaConversions._
      import java.io.FileReader

      val reader = new CSVReader(new FileReader(filename))
      val header = reader.readNext.map(_.trim)
      val rawData = reader.readAll.map(row => header.zip(row.map(_.trim)).toMap).toList

      new CSVDataSource(sc.parallelize(rawData), header, metaSource)
    }

    def runQuery(dataSources: Set[DataSource], metaEntries: Set[MetaEntry]): Iterator[DataSource] = {
      new Query(dataSources, metaEntries).run
    }
  }

  /*
   * DataSource implicit functions
   */

  implicit class DataSourceImplicits(ds: DataSource) {
    def saveToExistingCassandraTable = scrubjay.datasource.CassandraDataSource.DataSourceImplicits(ds).saveToExistingCassandraTable _
    def saveToNewCassandraTable = scrubjay.datasource.CassandraDataSource.DataSourceImplicits(ds).saveToNewCassandraTable _
    def saveAsCSVDataSource = scrubjay.datasource.CSVDataSource.DataSourceImplicits(ds).saveAsCSVDataSource _
  }

  /*
   * MetaSource implicit functions
   */

  implicit class MetaSource_saveCSV(m: MetaSource) {
    def saveAsCSVMetaSource = scrubjay.metasource.CSVMetaSource.MetaSource_saveCSV(m).saveAsCSVMetaSource _
  }

}
