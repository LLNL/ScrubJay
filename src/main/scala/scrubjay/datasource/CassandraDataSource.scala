package scrubjay.datasource

import scrubjay.metasource._
import scrubjay.metabase.MetaDescriptor._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkContext
import org.joda.time.{DateTime, Interval}

import scala.reflect._

/*
trait CassandraDataSource {

  val keyspace: String
  val table: String

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
*/

case class CassandraDataSource(keyspace: String,
                               table: String,
                               metaSourceID: MetaSourceID = MetaSourceID.empty,
                               selectColumns: Seq[String] = Seq.empty,
                               whereConditions: Seq[String] = Seq.empty,
                               limit: Option[Long] = None)
  extends DataSourceID {

  // TODO: figure out which columns to include
  val metaSource: MetaSource = metaSourceID.realize//.withColumns(cassandraRdd.selectedColumnRefs.map(_.toString))

  def isValid: Boolean = true

  def realize: ScrubJayRDD = {
    val selectStatement: Option[String] = if (selectColumns.nonEmpty) Some(selectColumns.mkString(", ")) else None

    val providedCassandraRdd = SparkContext.getOrCreate().cassandraTable(keyspace, table)

    val cassandraRdd = {
      val cassRddSelect = selectStatement.foldLeft(providedCassandraRdd)(_.select(_))
      val cassRddSelectWhere = whereConditions.foldLeft(cassRddSelect)(_.where(_))
      val cassRddSelectWhereLimit = limit.foldLeft(cassRddSelectWhere)(_.limit(_))
      cassRddSelectWhereLimit
    }

    val rawRdd = cassandraRdd.map(_.toMap.filter{case (_, null) => false; case _ => true})

    new ScrubJayRDD(rawRdd, metaSource)
  }
}

object CassandraDataSource {

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
  def cassandraSchemaForDataSource(dsID: DataSourceID): Seq[(String, String)] = {
    dsID.metaSource.map{case (c, me) => ("\"" + c + "\"", inferCassandraTypeString(me.units))}.toSeq
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

  def saveToCassandra(dsID: DataSourceID,
                      keyspace: String,
                      table: String): Unit = {

    // Convert rows to CassandraRow instances and save to the table
    dsID.realize.map(row => CassandraRow.fromMap(row.map(kv => kv._1 -> kv._2.rawString)))
      .saveToCassandra(keyspace, table)
  }

  def createCassandraTable(dsID: DataSourceID,
                           keyspace: String,
                           table: String,
                           primaryKeys: Seq[String],
                           clusterKeys: Seq[String]): Unit = {

    // Infer the schema from the DataSource
    val schema = cassandraSchemaForDataSource(dsID)

    // Generate CQL commands for creating/inserting meta information
    val CQLCommand = createCassandraTableCQL(keyspace, table, schema, primaryKeys, clusterKeys)

    // Run the generated CQL commands
    CassandraConnector(SparkContext.getOrCreate().getConf).withSessionDo { session =>
      session.execute(CQLCommand)
    }
  }
}
