package scrubjay

/*
 * Single entry point for ScrubJay API
 *
 * All functions here are meant to be externally accessible via `import scrubjay.imports._`
 *
 */

object imports {

  /*
   * Types
   */

  type ScrubJaySession = scrubjay.ScrubJaySession
  type MetaSource = scrubjay.meta.MetaSource
  type MetaEntry = scrubjay.meta.MetaEntry
  type DataSource = scrubjay.datasource.DataSource
  type Query = scrubjay.query.Query

  type NaturalJoin = scrubjay.derivation.NaturalJoin

  /*
   * Standalone functions
   */

  def metaEntryFromStrings = scrubjay.meta.MetaEntry.metaEntryFromStrings _

  def createCSVMetaSource = scrubjay.metasource.CSVMetaSource.createCSVMetaSource _
  def createLocalMetaSource = scrubjay.metasource.LocalMetaSource.createLocalMetaSource _

  def deriveTimeSpan = scrubjay.derivation.DeriveTimeSpan.deriveTimeSpan _
  def deriveExplodeList = scrubjay.derivation.ExplodeList.deriveExplodeList _

  /*
   * ScrubJaySession implicit functions
   */

  implicit class ScrubJaySessionImplicits(sjs: ScrubJaySession) {
    def createCassandraDataSource = scrubjay.datasource.CassandraDataSource.ScrubJaySessionImplicits(sjs).createCassandraDataSource _
    def createCSVDataSource = scrubjay.datasource.CSVDataSource.ScrubJaySessionImplicits(sjs).createCSVDataSource _
    def createLocalDataSource = scrubjay.datasource.LocalDataSource.ScrubJaySessionImplicits(sjs).createLocalDataSource _
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
