// Scala
import scala.collection.immutable.Map
import scala.collection.JavaConverters._

// Spark
import org.apache.spark.SparkContext
import org.apache.spark.Partition
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD

// Datastax
import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd._
import com.datastax.spark.connector.cql.CassandraConnector

// ScrubJay
import scrubjay.datasource._

package scrubjay {

  object cassandra {

    // Match Scala type to Cassandra type string
    def InferCassandraTypeString(v: Any): String = v match {
      case _: String     => "text"
      case _: Int        => "int"
      case _: Float      => "float"
      case _: Double     => "double"
      case _: BigDecimal => "decimal"
      case _: BigInt     => "varint"

      // Cassandra collections, assume at least one element if column is defined
      case l: List[_]  => "list<" + InferCassandraTypeString(l(0)) + ">"
      case s: Set[_]   => "set<"  + InferCassandraTypeString(s.head) + ">"
      case m: Map[_,_] => "map<"  + InferCassandraTypeString(m.head._1) + "," + InferCassandraTypeString(m.head._2) + ">"
    }

    // Get columns and datatypes from the data and add meta_value and meta_units for each column
    def DataSourceCassandraSchema(ds: DataSource): List[(String, String)] = {
      ds.Data
        .reduce((m1, m2) => m1 ++ m2)
        .flatMap{case (s, v) => 
          Seq((s, InferCassandraTypeString(v)), 
              (s"meta_$s", "tuple<int,int>"))}
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

    // The sequence of CQL commands to insert meta entries into the Cassandra meta table
    def CreateMetaEntriesCQL(keyspace: String,
                             table: String,
                             schema: List[(String, String)], 
                             primaryKey: String,
                             metaMap: MetaMap): List[String] = {
      val reverseMetaMap = metaMap.map(_.swap)

      // For each data column, create: 
      schema.filterNot(_._1 startsWith "meta_")
        .flatMap(column => {
          val columnName = column._1
          val metaEntry = reverseMetaMap(columnName)
          List(
            // 1. a meta reference in the data table
            s"""
            |INSERT INTO $keyspace.$table ($primaryKey, meta_${columnName})
            |VALUES (0, (${metaEntry.value.hashCode}, ${metaEntry.units.hashCode}))
            |""".stripMargin.replaceAll("\n"," "),

            // 2. a meta value entry in the meta table
            s"""
            |INSERT INTO $keyspace.meta (meta_key, title, description)
            |VALUES (${metaEntry.value.hashCode}, \'${metaEntry.value.title}\', \'${metaEntry.value.description}\')
            |""".stripMargin.replaceAll("\n"," "),

            // 3. a meta units entry in the meta table
            s"""
            |INSERT INTO $keyspace.meta (meta_key, title, description)
            |VALUES (${metaEntry.units.hashCode}, \'${metaEntry.units.title}\', \'${metaEntry.units.description}\')
            |""".stripMargin.replaceAll("\n"," ")
        )})
    }

    implicit class CassandraDataSourceWriter(ds: DataSource) {
      def saveToCassandra(sc: SparkContext, keyspace: String, table: String) {

        // Infer the schema from the DataSource
        val schema = DataSourceCassandraSchema(ds)

        // Choose a primary key (first column for now)
        val primaryKey = schema.filterNot{case (k,v) => k startsWith "meta_"}.head._1
        
        // Generate CQL commands for creating/inserting meta information
        val CQLcommands = CreateCassandraDataTableCQL(keyspace, table, schema, primaryKey) +: 
                          CreateMetaEntriesCQL(keyspace, table, schema, primaryKey, ds.Meta)

        // Run the generated CQL commands
        CassandraConnector(sc.getConf).withSessionDo { session =>
          for (CQLcmd <- CQLcommands) {
            println(CQLcmd)
            session.execute(CQLcmd)
          }
        }

        // Create meta columns with None entries (will be ignored from writes)
        val metaColumns = schema.map(_._1)
          .filter(_ startsWith "meta_")
          .map(meta => Map(meta -> None))
          .reduce((a,b) => a ++ b)

        // Convert rows to CassandraRow instances and save to the table
        ds.Data.map(_ ++ metaColumns)
          .map(CassandraRow.fromMap(_))
          .saveToCassandra(keyspace, table)
      }
    }

    class CassandraDataSource(val sc: SparkContext, 
                              val keyspace: String, 
                              val table: String) extends DataSource {

      lazy val cassandra_data_table = sc.cassandraTable(keyspace, table)
      lazy val cassandra_meta_table = sc.cassandraTable(keyspace, "meta")

      lazy val meta_columns = cassandra_data_table.selectedColumnRefs.filter(_.toString startsWith "meta_")
      lazy val data_columns = cassandra_data_table.selectedColumnRefs.filterNot(_.toString startsWith "meta_")

      lazy val Meta: MetaMap = {
        cassandra_data_table.select(meta_columns:_*)            // select meta_xxx columns
          .map(_.toMap).reduce((a,b) => a ++ b)                 // Map(meta_xxx -> (int, int))
          .mapValues{case t: TupleValue =>                      
            MetaEntry(MetaDescriptorLookup(t.getInt(0)), 
                      MetaDescriptorLookup(t.getInt(1)))}       // Map(meta_xxx -> MetaEntry)
          .map{case (k, v) => (v, k.substring("meta_".length))} // Map(MetaEntry -> xxx)
      }

      lazy val Data: RDD[DataRow] = {
        cassandra_data_table.select(data_columns:_*).map(_.toMap)
      }
    }
  }
}
