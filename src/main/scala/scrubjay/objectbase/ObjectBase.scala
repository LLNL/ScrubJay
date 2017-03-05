package scrubjay.objectbase

import scrubjay.datasource.{CassandraDataSource, DataSourceID, ScrubJayRDD}
import scrubjay.ScrubJaySessionImplicits
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import org.apache.spark.SparkContext
import scrubjay.metasource.CassandraMetaSource

object ObjectBase {

  /*
   * An ObjectBase is a table that describes a set of meta-annotated data sources (ScrubJayRDD instances)
   *  "original" objects are data sources that have not been derived, only recorded
   *  "derived" objects are data source that were derived from other data sources (original or derived)
   *
   * It has the schema:
   *  name, metaKeyspace, metaTable, dataKeyspace, dataTable
   *
   * For example:
   *  "cabDat2016Temperature", "cab_dat_2016", "temp_meta", "cab_dat_2016", "temp_data"
   *
   */

  final val SCRUBJAY_OBJECTS_KEYSPACE = "scrubjay_objectbases"
  final val SCRUBJAY_ORIGINAL_OBJECTS_TABLE = "original"
  final val SCRUBJAY_DERIVED_OBJECTS_TABLE = "derived"

  final val SCRUBJAY_DATA_KEYSPACE = "scrubjay_databases"

  final val SCRUBJAY_META_KEYSPACE = "scrubjay_metabases"

  final val OBJECT_COLUMN_NAME = "name"
  final val OBJECT_COLUMN_META_KEYSPACE = "metaKeyspace"
  final val OBJECT_COLUMN_META_TABLE = "metaTable"
  final val OBJECT_COLUMN_DATA_KEYSPACE = "dataKeyspace"
  final val OBJECT_COLUMN_DATA_TABLE = "dataTable"

  private def loadObjects(crdd: CassandraTableScanRDD[CassandraRow]): Map[String, ScrubJayRDD] = {

    val sc = crdd.sparkContext

    val names = crdd.map(row =>
      ( row.getString(OBJECT_COLUMN_NAME),
        row.getString(OBJECT_COLUMN_META_KEYSPACE),
        row.getString(OBJECT_COLUMN_META_TABLE),
        row.getString(OBJECT_COLUMN_DATA_KEYSPACE),
        row.getString(OBJECT_COLUMN_DATA_TABLE)
      )).collect.toSeq

    names.map{
      case (k, metaKeyspace, metaTable, dataKeyspace, dataTable) => {
        val metaSource = CassandraMetaSource(metaKeyspace, metaTable)
        (k, CassandraDataSource(dataKeyspace, dataTable, metaSource).asInstanceOf[ScrubJayRDD])
      }
    }.toMap
  }

  def loadOriginalObjects(sc: SparkContext): Map[String, ScrubJayRDD] = {
    loadObjects(sc.cassandraTable(SCRUBJAY_OBJECTS_KEYSPACE, SCRUBJAY_ORIGINAL_OBJECTS_TABLE))
  }

  def loadDerivedObjects(sc: SparkContext): Map[String, ScrubJayRDD] = {
    loadObjects(sc.cassandraTable(SCRUBJAY_OBJECTS_KEYSPACE, SCRUBJAY_DERIVED_OBJECTS_TABLE))
  }

  def saveAsOriginalObject(dsID: DataSourceID,
                           metaKeyspaceTableTuple: Option[(String, String)],
                           dataKeyspaceTableTuple: Option[(String, String)],
                           saveObjectReferenceOnly: Boolean = false): Unit = {
    val (dataKeyspace, dataTable) = dataKeyspaceTableTuple.getOrElse((SCRUBJAY_DATA_KEYSPACE, /* FIXME + */ "_data"))
    val (metaKeyspace, metaTable) = metaKeyspaceTableTuple.getOrElse((SCRUBJAY_META_KEYSPACE, /* FIXME + */ "_meta"))

    val CQLCommand =
      s"INSERT INTO $SCRUBJAY_OBJECTS_KEYSPACE.$SCRUBJAY_ORIGINAL_OBJECTS_TABLE " +
      s"($OBJECT_COLUMN_NAME, $OBJECT_COLUMN_META_KEYSPACE, $OBJECT_COLUMN_META_TABLE, $OBJECT_COLUMN_DATA_KEYSPACE, $OBJECT_COLUMN_DATA_TABLE) " +
      s"VALUES (name, $metaKeyspace, $metaTable, $dataKeyspace, $dataTable)" /* FIXME */

    CassandraConnector(SparkContext.getOrCreate().getConf).withSessionDo { session =>
      session.execute(CQLCommand)
    }

    if (!saveObjectReferenceOnly) {
      // FIXME
      //dsID.metaSource.saveToCassandra(sc, metaKeyspace, name)
      //ds.saveToCassandra(dataKeyspace, name)
    }

  }
}
