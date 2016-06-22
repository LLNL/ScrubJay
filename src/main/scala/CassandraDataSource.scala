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
              (s"meta_value_$s", "varint"),
              (s"meta_units_$s", "varint"))}
        .toList
    }

    // The CQL command to create the Cassandra meta table
    def CreateCassandraMetaTableCQL(keyspace: String): String = {
      s"CREATE TABLE IF NOT EXISTS $keyspace.meta (meta_key varint PRIMARY KEY, title text, description text)"
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
            |INSERT INTO $keyspace.$table ($primaryKey, meta_units_${columnName}, meta_value_${columnName}) 
            |VALUES (0, ${metaEntry.units.hashCode}, ${metaEntry.value.hashCode})
            |""".stripMargin.replaceAll("\n"," "),

            // 2. a meta units entry in the meta table
            s"""
            |INSERT INTO $keyspace.meta (meta_key, title, description)
            |VALUES (${metaEntry.units.hashCode}, \'${metaEntry.units.title}\', \'${metaEntry.units.description}\')
            |""".stripMargin.replaceAll("\n"," "),

            // 3. a meta value entry in the meta table
            s"""
            |INSERT INTO $keyspace.meta (meta_key, title, description)
            |VALUES (${metaEntry.value.hashCode}, \'${metaEntry.value.title}\', \'${metaEntry.value.description}\')
            |""".stripMargin.replaceAll("\n"," ")
        )})
    }

    implicit class CassandraDataSourceWriter(ds: DataSource) {
      def saveToCassandra(sc: SparkContext, keyspace: String, table: String) {

        val schema = DataSourceCassandraSchema(ds)

        // default primary key is first column
        val primaryKey = schema(0)._1 
        
        val CQLcommands = CreateCassandraMetaTableCQL(keyspace) +: 
                          CreateCassandraDataTableCQL(keyspace, table, schema, primaryKey) +: 
                          CreateMetaEntriesCQL(keyspace, table, schema, primaryKey, ds.Meta)

        CassandraConnector(sc.getConf).withSessionDo { session =>
          for (CQLcmd <- CQLcommands) {
            println(CQLcmd)
            session.execute(CQLcmd)
          }
        }
      }
    }

    class CassandraDataSource(val sc: SparkContext, 
                              val keyspace: String, 
                              val table: String) extends DataSource {

      lazy val cassandra_data_table = sc.cassandraTable(keyspace, table)
      lazy val cassandra_meta_table = sc.cassandraTable(keyspace, "meta")

      lazy val Meta: MetaMap = {

        val data_columns = cassandra_data_table.selectedColumnNames
        val meta_columns = cassandra_meta_table.selectedColumnNames

        val data_meta_columns = data_columns
          .filter(!_.startsWith("meta_"))
          .flatMap(col => Seq(ColumnName("meta_attribute_"+col), ColumnName("meta_units_"+col)))

        val data_meta_keys = cassandra_data_table.select(data_meta_columns:_*).collect()

        println(data_meta_keys)
        
        Map(MetaEntry(MetaValue("", ""), MetaUnits("", "")) -> "blah")
      }

      lazy val Data: RDD[DataRow] = {
        cassandra_data_table.map(_.toMap)
      }
    }
  }
}
