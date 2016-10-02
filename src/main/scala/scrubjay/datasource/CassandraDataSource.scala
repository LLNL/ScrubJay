package scrubjay.datasource

import scrubjay._
import scrubjay.meta._
import scrubjay.units.Units
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.rdd.CassandraTableScanRDD

import scala.reflect._
import scala.reflect.runtime.universe.typeOf
import scala.reflect.runtime.universe.WeakTypeTag
import scala.reflect.ClassTag

class CassandraDataSource(sc: SparkContext,
                          val keyspace: String,
                          val table: String,
                          providedMetaSource: MetaSource,
                          val metaBase: MetaBase,
                          select: Option[String] = None,
                          where: Option[String] = None) extends DataSource  {

  def addSecondaryIndex(sc: SparkContext,
                        column: String): Unit = {
    CassandraConnector(sc.getConf).withSessionDo { session =>
      session.execute(s"CREATE INDEX ON $keyspace $table ($column)")
    }
  }

  val cassandraRdd: CassandraTableScanRDD[CassandraRow] = {
    val cassRdd = sc.cassandraTable(keyspace, table)
    val cassRddSelected = select.fold(cassRdd)(cassRdd.select(_))
    val cassRddSelectWhere = where.fold(cassRddSelected)(cassRddSelected.where(_))
    cassRddSelectWhere
  }

  val metaSource = providedMetaSource.withColumns(cassandraRdd.selectedColumnRefs.map(_.toString))

  lazy val rdd: RDD[DataRow] = {
    Units.rawRDDToUnitsRDD(sc, cassandraRdd.map(_.toMap), metaSource.metaEntryMap)
  }
}

object CassandraDataSource {

  // Match Scala type to Cassandra type string
   def inferCassandraTypeString[T](v: WeakTypeTag[T]): String = {
     v.tpe match {
       case t if t =:= typeOf[String] => "text"
       case t if t =:= typeOf[Int] => "int"
       case t if t =:= typeOf[Float] => "float"
       case t if t =:= typeOf[Double] => "double"
       case t if t =:= typeOf[Double] => "decimal"
       case t if t =:= typeOf[BigInt] => "varint"

       // Cassandra collections
       // case x if x == classTag[List[_]]  => "list<" + inferCassandraTypeString(l.head) + ">"
       // case x if x == classTag[Set[_]]   => "set<"  + inferCassandraTypeString(s.head) + ">"
       // case x if x == classTag[Map[_,_]] => "map<"  + inferCassandraTypeString(m.head._1) + "," + inferCassandraTypeString(m.head._2) + ">"

       case unk => throw new RuntimeException(s"Unable to infer Cassandra data type for $unk")
     }
   }

  // Get columns and datatypes from the data and add meta_data for each column
  // FIXME: Determine schema from metaSource
  def cassandraSchemaForDataSource(ds: DataSource): List[(String, String)] = {
    ds.metaSource.metaEntryMap.map{case (c, me) => (c, inferCassandraTypeString(me.units.weaktypetag))}.toList
  }

  // The CQL command to create a Cassandra table with the specified schema
  def createCassandraTableCQL(keyspace: String,
                              table: String,
                              schema: List[(String, String)],
                              primaryKeys: List[String],
                              clusterKeys: List[String]): String = {

    val schemaString = schema.map{case (s, v) => s"$s $v"}.mkString(", ")
    val primaryKeyString = primaryKeys.mkString(",")
    val clusterKeyString = clusterKeys.mkString(",")

    s"CREATE TABLE $keyspace.$table ($schemaString, PRIMARY KEY (($primaryKeyString), ($clusterKeyString)))"
  }

  implicit class DataSource_SaveToCassandra(ds: DataSource) {

    def saveToExistingCassandraTable(sc: SparkContext,
                                     keyspace: String,
                                     table: String): Unit = {

      // Convert rows to CassandraRow instances and save to the table
      ds.rdd.map(CassandraRow.fromMap(_))
        .saveToCassandra(keyspace, table)
    }

    def saveToNewCassandraTable(sc: SparkContext,
                                keyspace: String,
                                table: String,
                                primaryKeys: List[String],
                                clusterKeys: List[String]): Unit = {

      // Infer the schema from the DataSource
      val schema = cassandraSchemaForDataSource(ds)

      // Generate CQL commands for creating/inserting meta information
      val CQLCommand = createCassandraTableCQL(keyspace, table, schema, primaryKeys, clusterKeys)

      // Run the generated CQL commands
      CassandraConnector(sc.getConf).withSessionDo { session =>
        session.execute(CQLCommand)
      }

      saveToExistingCassandraTable(sc, keyspace, table)
    }
  }

  implicit class ScrubJaySession_CassandraDataSource(sjs: ScrubJaySession) {
    def createCassandraDataSource(keyspace: String,
                                  table: String,
                                  metaSource: MetaSource = new EmptyMetaSource,
                                  select: Option[String] = None,
                                  where: Option[String] = None): CassandraDataSource = {
      new CassandraDataSource(sjs.sc, keyspace, table, metaSource, sjs.metaBase, select, where)
    }
  }
}
