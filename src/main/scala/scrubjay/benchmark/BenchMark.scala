package scrubjay.benchmark

import org.apache.spark.sql.SparkSession
import scrubjay.datasetid.combination.InterpolationJoin
import scrubjay.util.returnTime

trait BenchMark[T] {

  protected val argGenerator: Iterator[T]

  protected def bench(arg: T): Seq[Any]

  private def warmup(spark: SparkSession): Unit = {
    val numRows = 1000
    val timeTemp = GenerateInputs.timeXTemp(numRows)
    val timeFlops = GenerateInputs.timeXFlops(numRows)

    val warmup = {
      lazy val interjoined = InterpolationJoin(timeTemp, timeFlops, 6)
      val t = returnTime(interjoined.realize(GenerateInputs.dimensionSpace).collect())
      println(t)
    }

    spark.sqlContext.clearCache()
  }

  def run(spark: SparkSession): Unit = {

    spark.sparkContext.setLogLevel("WARN")

    // Warmup Spark
    println("Warming up Spark...")
    warmup(spark)
    println("Done! Running benchmarks...")

    // Collect benchmark results
    val benchResults = argGenerator.map(bench)

    // Print benchmark results
    benchResults.foreach(results => results.foreach(println))

    spark.stop()
  }

}
