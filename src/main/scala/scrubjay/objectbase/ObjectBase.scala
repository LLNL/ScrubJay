package scrubjay.objectbase

import com.datastax.spark.connector._
import org.apache.spark.SparkContext
import scrubjay.datasource.ScrubJayRDD

import scrubjay.ScrubJaySessionImplicits
import scrubjay.createCassandraMetaSource

object ObjectBase {

  /*
   * An ObjectBase is a table that describes a set of meta-annotated data sources (ScrubJayRDD instances)
   *  "original" objects are data sources that have not been derived, only recorded
   *  "derived" objects are data source that were derived from other data sources (original or derived)
   *
   * It has the schema:
   *  name, metaTable, dataTable
   *
   * For example:
   *  "cabDat2016Temperature", "cab_dat_2016.temp_meta", "cab_dat_2016.temp"
   *
   */

  private def loadDataObjectsFromNames(sc: SparkContext, names: Map[String, (String, String)]): Map[String, ScrubJayRDD] = {
    names.map{
      case (k, (metaFullTableName, dataFullTableName)) => {

        val metaSplitNames = metaFullTableName.split(".")
        val (metaKeyspace, metaTableName) = (metaSplitNames(0), metaSplitNames(1))

        val dataSplitNames = dataFullTableName.split(".")
        val (dataKeyspace, dataTableName) = (dataSplitNames(0), dataSplitNames(1))

        val metaSource = createCassandraMetaSource(sc, metaKeyspace, metaTableName)
        (k, sc.createCassandraDataSource(dataKeyspace, dataTableName, metaSource).get.asInstanceOf[ScrubJayRDD])
      }
    }
  }

  def loadOriginalObjects(sc: SparkContext): Map[String, ScrubJayRDD] = {
    val originalSourcesRdd = sc.cassandraTable("scrubjay", "original_sources")
    val originalSourcesNames: Map[String, (String, String)] = originalSourcesRdd.map(row =>
      (row.get[String](0), (row.get[String](1), row.get[String](2)))).collect.toMap
    loadDataObjectsFromNames(sc, originalSourcesNames)
  }

  def loadDerivedObjects(sc: SparkContext): Map[String, ScrubJayRDD] = {
    // "spec", "metaSourceTable", "dataSourceTable"
    val derivedSourcesRdd = sc.cassandraTable("scrubjay", "derived_sources")
    val derivedSourcesNames: Map[String, (String, String)] = derivedSourcesRdd.map(row =>
      (row.get[String](0), (row.get[String](1), row.get[String](2)))).collect.toMap
    loadDataObjectsFromNames(sc, derivedSourcesNames)
  }
}
