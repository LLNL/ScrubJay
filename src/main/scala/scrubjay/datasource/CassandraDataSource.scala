package scrubjay.datasource

import scrubjay.util.niceAttempt
import scrubjay.metabase.MetaDescriptor._
import scrubjay.metasource.MetaSource
import scrubjay.units.Units
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.rdd.CassandraTableScanRDD
import org.apache.spark.SparkContext
import com.github.nscala_time.time.Imports._

import scala.reflect._

class CassandraDataSource(cassandraRdd: CassandraTableScanRDD[CassandraRow],
                          providedMetaSource: MetaSource)
  extends DataSource {

  override lazy val metaSource = providedMetaSource.withColumns(cassandraRdd.selectedColumnRefs.map(_.toString))
  override lazy val rdd = Units.rawRDDToUnitsRDD(cassandraRdd.map(_.toMap.filter{case (k, null) => false; case _ => true}), metaSource.metaEntryMap)

  val keyspace = cassandraRdd.keyspaceName
  val table = cassandraRdd.tableName

  def addSecondaryIndex(sc: SparkContext,
                        column: String): Unit = {
    CassandraConnector(sc.getConf).withSessionDo { session =>
      session.execute(s"CREATE INDEX ON $keyspace $table ($column)")
    }
  }

  def addMaterializedView(sc: SparkContext,
                          newKeyspace: String,
                          newTable: String,
                          primaryKeys: Seq[String],
                          clusterKeys: Seq[String],
                          selectColumns: Seq[String],
                          whereConditions: Seq[String]): Unit = {

    val selectClause = if (selectColumns.nonEmpty) selectColumns.mkString(", ") else "*"
    val whereClause = if (whereConditions.nonEmpty) "WHERE " + whereConditions.mkString(" AND ") else ""
    val clusterKeyString =  if (clusterKeys.nonEmpty) ", " + clusterKeys.mkString(",") else ""
    val primaryKeyString = primaryKeys.mkString(",")

    val CQLCommand =
      s"CREATE MATERIALIZED VIEW $newKeyspace.$newTable" +
      s" AS SELECT $selectClause" +
      s" FROM $keyspace.$table" +
      s" $whereClause" +
      s" PRIMARY KEY (($primaryKeyString)$clusterKeyString)"

    CassandraConnector(sc.getConf).withSessionDo { session =>
      session.execute(CQLCommand)
    }
  }

}

object CassandraDataSource {

  def createCassandraDataSource(providedCassandraRdd: CassandraTableScanRDD[CassandraRow],
                                metaSource: MetaSource,
                                selectColumns: Seq[String],
                                whereConditions: Seq[String],
                                limit: Option[Long]): Option[CassandraDataSource] = {

    niceAttempt {

      val cassandraRdd = {
        val cassRddSelect = selectColumns.foldLeft(providedCassandraRdd)(_.select(_))
        val cassRddSelectWhere = whereConditions.foldLeft(cassRddSelect)(_.where(_))
        val cassRddSelectWhereLimit = limit.foldLeft(cassRddSelectWhere)(_.limit(_))
        cassRddSelectWhereLimit
      }

      new CassandraDataSource(cassandraRdd, metaSource)
    }
  }

  // Match Scala type to Cassandra type string
  def inferCassandraTypeString(metaUnits: MetaUnits): String = {
    metaUnits.unitsTag.rawValueClassTag match {
      case t if t == classTag[String] => "text"
      case t if t == classTag[Int] => "int"
      case t if t == classTag[Float] => "float"
      case t if t == classTag[Double] => "double"
      case t if t == classTag[Double] => "decimal"
      case t if t == classTag[Long] => "bigint"
      case t if t == classTag[BigInt] => "bigint"
      case t if t == classTag[DateTime] => "timestamp"
      case t if t == classTag[Interval] => "tuple<timestamp, timestamp>"

      // Cassandra collections
      case t if t == classTag[List[_]]  => "list<" + inferCassandraTypeString(metaUnits.unitsChildren.head) + ">"
      case t if t == classTag[Set[_]]   => "set<"  + inferCassandraTypeString(metaUnits.unitsChildren.head) + ">"
      case t if t == classTag[Map[_,_]] => "map<"  + inferCassandraTypeString(metaUnits.unitsChildren.head) + "," +
                                                     inferCassandraTypeString(metaUnits.unitsChildren(1)) + ">"

      case unk => throw new RuntimeException(s"Unable to infer Cassandra data type for $unk")
   }
 }

  // Get columns and datatypes from the data and add meta_data for each column
  def cassandraSchemaForDataSource(ds: DataSource): Seq[(String, String)] = {
    ds.metaSource.metaEntryMap.map{case (c, me) => ("\"" + c + "\"", inferCassandraTypeString(me.units))}.toSeq
  }

  // The CQL command to create a Cassandra table with the specified schema
  def createCassandraTableCQL(keyspace: String,
                              table: String,
                              schema: Seq[(String, String)],
                              primaryKeys: Seq[String],
                              clusterKeys: Seq[String]): String = {

    val schemaString = schema.map{case (s, v) => s"$s $v"}.mkString(", ")
    val clusterKeyString =  if (clusterKeys.nonEmpty) ", " + clusterKeys.mkString(",") else ""
    val primaryKeyString = primaryKeys.mkString(",")

    s"CREATE TABLE $keyspace.$table ($schemaString, PRIMARY KEY (($primaryKeyString)$clusterKeyString))"
  }

  def saveToCassandra(ds: DataSource,
                      keyspace: String,
                      table: String): Unit = {

    // Convert rows to CassandraRow instances and save to the table
    ds.rdd.map(row => CassandraRow.fromMap(row.map(kv => kv._1 -> kv._2.rawString)))
      .saveToCassandra(keyspace, table)
  }

  def createCassandraTable(ds: DataSource,
                           keyspace: String,
                           table: String,
                           primaryKeys: Seq[String],
                           clusterKeys: Seq[String]): Unit = {

    // Infer the schema from the DataSource
    val schema = cassandraSchemaForDataSource(ds)

    // Generate CQL commands for creating/inserting meta information
    val CQLCommand = createCassandraTableCQL(keyspace, table, schema, primaryKeys, clusterKeys)

    // Run the generated CQL commands
    CassandraConnector(ds.rdd.sparkContext.getConf).withSessionDo { session =>
      session.execute(CQLCommand)
    }
  }
}
