// Scala
import scala.collection.immutable.Map

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
  class CassandraDataSource(val sc: SparkContext, 
                            val keyspace: String, 
                            val table: String) extends DataSource {

    lazy val Meta: MetaMap = {
      // Get meta from somewhere
      Map(new MetaEntry(new MetaValue("", ""), new MetaUnits("", "")) -> "blah")
    }
    lazy val Data: RDD[DataRow] = {
      // Open keyspace.table and convert to Map[String, Any]
      sc.cassandraTable(keyspace, table).map(_.toMap)
    }
  }
}
