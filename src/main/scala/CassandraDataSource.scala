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

// ScrubJay
import scrubjay.datasource._

package scrubjay {

  object cassandra {

    def inferCassandraTypeString(v: Any): String = v match {
      case _: String     => "text"
      case _: Int        => "int"
      case _: Float      => "float"
      case _: Double     => "double"
      case _: BigDecimal => "decimal"
      case _: BigInt     => "varint"

      // Cassandra collections, assume at least one element if column is defined
      case l: List[_]  => "list<" + inferCassandraTypeString(l(0)) + ">"
      case s: Set[_]   => "set<"  + inferCassandraTypeString(s.head) + ">"
      case m: Map[_,_] => "map<"  + inferCassandraTypeString(m.head._1) + "," + inferCassandraTypeString(m.head._2) + ">"
    }

    implicit class CassandraDataSourceWriter(ds: DataSource) {
      def saveToCassandra(sc: SparkContext, keyspace: String, table: String) {

        // Get columns and datatypes from the data
        // and add meta_value and meta_units for each column
        val schema = ds.Data
          .reduce((m1, m2) => m1 ++ m2)
          .flatMap{case (s, v) => 
            Seq((s, inferCassandraTypeString(v)), 
                (s"meta_value_$s", "varint"),
                (s"meta_units_$s", "varint"))}
          .toList

        // Schema as needed for CQL command
        val schemaString = schema.map{case (s, v) => s"$s $v"}.mkString(", ")

        // Choose first column as primary key (default)
        val primaryKey = schema(0)._1

        // The command to create the table with specified schema 
        val createTableCQLcmd = s"CREATE TABLE $keyspace.$table ($schemaString, PRIMARY KEY ($primaryKey))" 

        // Find the matching meta entries for each column
        val reverseMetaMap = ds.Meta.map(_.swap)

        // The command to insert meta rows into table
        val insertMetaCQLcmd = s"INSERT INTO $keyspace.$table ("


        // Create meta entries in meta table
        // Save data to Cassandra
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
