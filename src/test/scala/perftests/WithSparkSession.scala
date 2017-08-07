package perftests

import org.apache.spark.sql.SparkSession

trait WithSparkSession {
  var spark: SparkSession = _

  def startSpark(master: String = "local[*]"): Unit = {
    spark = SparkSession.builder()
      .appName("ScrubJayPerformanceTests")
      .master(master)
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
  }

  def stopSpark: Unit = {
    spark.stop()
  }
}
