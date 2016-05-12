// Scala
import scala.util.parsing.json._
import scala.collection.immutable.Map
import scala.Enumeration

// Spark
import org.apache.spark.SparkContext
import org.apache.spark.Partition
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD

// Datastax
import com.datastax.spark.connector._
import com.datastax.spark.connector.rdd._

package scrubjay {

  class Meta(val localname: String, val globalname: String, val units: String) {

  }

  class DataSource(val rdd: RDD[CassandraRow], val meta: Array[Meta]) {

  }

  // Create a join function that takes as input:
  // 1) a similarity function
  // 2) a merging function 
}
