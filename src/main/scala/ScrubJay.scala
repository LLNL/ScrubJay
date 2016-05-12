// Spark
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

// Datastax
import com.datastax.spark.connector._

package scrubjay {

  class ScrubJaySession(val hostname: String = "localhost",
                        val username: String = "cassandra",
                        val password: String = "cassandra") {

    val sparkConf = new SparkConf(true)
      .set("spark.cassandra.connection.host", hostname)
      .set("spark.cassandra.auth.username", username)            
      .set("spark.cassandra.auth.password", password)            
      .set("spark.app.id", "ScrubJayAppID")
    val sc = new SparkContext("local[*]", "ScrubJay", sparkConf)
    val sqlc = new SQLContext(sc)
  }
}
