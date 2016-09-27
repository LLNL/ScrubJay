package scrubjay.datasource

import scrubjay._
import scrubjay.meta._
import scrubjay.units.Units
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.rdd.CassandraTableScanRDD

class CassandraDataSource(metaOntology: MetaBase,
                          metaMap: MetaMap,
                          keyspace: String,
                          table: String,
                          sc: SparkContext,
                          select: Option[String] = None,
                          where: Option[String] = None) extends OriginalDataSource(metaOntology, metaMap)  {

  val cassandraRdd: CassandraTableScanRDD[CassandraRow] = {
    val cassRdd = sc.cassandraTable(keyspace, table)
    val cassRddSelected = select.fold(cassRdd)(cassRdd.select(_))
    val cassRddSelectWhere = where.fold(cassRddSelected)(cassRddSelected.where(_))
    cassRddSelectWhere
  }

  override val metaEntryMap = cassandraRdd.selectedColumnRefs.map(_.toString)
    .map(col => col -> metaMap.getOrElse(col, MetaEntry.fromStringTuple("unknown", "unknown", "identifier"))).toMap

  lazy val rdd: RDD[DataRow] = {
    Units.rawRDDToUnitsRDD(sc, cassandraRdd.map(_.toMap), metaMap)
  }
}

object CassandraDataSource {

  // Match Scala type to Cassandra type string
  def InferCassandraTypeString(v: Any): String = v match {
    case _: String     => "text"
    case _: Int        => "int"
    case _: Float      => "float"
    case _: Double     => "double"
    case _: BigDecimal => "decimal"
    case _: BigInt     => "varint"

    // Cassandra "collections"
    case l: List[_]  => "list<" + InferCassandraTypeString(l.head) + ">"
    case s: Set[_]   => "set<"  + InferCassandraTypeString(s.head) + ">"
    case m: Map[_,_] => "map<"  + InferCassandraTypeString(m.head._1) + "," + InferCassandraTypeString(m.head._2) + ">"

    case unk => throw new RuntimeException(s"Unable to infer Cassandra data type for $unk")
  }

  // Get columns and datatypes from the data and add meta_data for each column
  // FIXME: is it possible for the reduction to create None value entries, e.g. ("jobid" -> None )?
  def DataSourceCassandraSchema(ds: DataSource): List[(String, String)] = {
    ds.rdd
      .reduce((m1, m2) => m1 ++ m2)
      .mapValues(InferCassandraTypeString(_))
      .toList
  }

  // The CQL command to create a Cassandra table with the specified schema
  def CreateCassandraDataTableCQL(keyspace: String, 
                                  table: String, 
                                  schema: List[(String, String)], 
                                  primaryKey: String): String = {
    val schemaString = schema.map{case (s, v) => s"$s $v"}.mkString(", ")
    s"CREATE TABLE $keyspace.$table ($schemaString, PRIMARY KEY ($primaryKey))" 
  }

  implicit class DataSource_SaveToCassandra(ds: DataSource) {
    def saveToCassandra(sc: SparkContext, keyspace: String, table: String) {

      // FIXME: default primary key? clustering order? secondary keys?
      
      // Infer the schema from the DataSource
      val schema = DataSourceCassandraSchema(ds)

      // Choose a primary key (first column for now)
      val primaryKey = schema.head._1
      
      // Generate CQL commands for creating/inserting meta information
      val CQLcmd = CreateCassandraDataTableCQL(keyspace, table, schema, primaryKey)

      // Run the generated CQL commands
      CassandraConnector(sc.getConf).withSessionDo { session =>
        println(CQLcmd)
        //session.execute(CQLcmd)
      }

      // Convert rows to CassandraRow instances and save to the table
      ds.rdd.map(CassandraRow.fromMap(_))
        //.saveToCassandra(keyspace, table)
    }
  }

  implicit class ScrubJaySession_CassandraDataSource(sjs: ScrubJaySession) {
    def createCassandraDataSource(keyspace: String,
                                  table: String,
                                  metaMap: MetaMap = Map[String, MetaEntry]().empty,
                                  select: Option[String] = None,
                                  where: Option[String] = None): CassandraDataSource = {
      new CassandraDataSource(sjs.metaOntology, metaMap, keyspace, table, sjs.sc, select, where)
    }
  }
}
