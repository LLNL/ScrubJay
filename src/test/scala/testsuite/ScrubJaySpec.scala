package testsuite

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSpec}

trait ScrubJaySpec extends FunSpec with BeforeAndAfterAll {

  var spark: SparkSession = _

  override protected def beforeAll: Unit = {
    spark = SparkSession.builder()
      .appName("ScrubJayFunctionalTests")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
  }

  override protected def afterAll: Unit = {
    spark.stop()
  }
}
