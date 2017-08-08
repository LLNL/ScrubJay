package scrubjay.benchmark

import org.apache.spark.sql.SparkSession

trait BenchMark[T] {

  protected val argGenerator: Iterator[T]

  protected def bench(arg: T): (T, Double)

  def run(spark: SparkSession): Unit = {

    spark.sparkContext.setLogLevel("WARN")

    // Warmup Spark
    println("Warming up Spark...")
    val v = GenerateInputs.timeXTemp(10000).realize(GenerateInputs.dimensionSpace).collect()
    SparkSession.builder().getOrCreate().sqlContext.clearCache()
    println("Ready!")

    // Collect benchmark results
    val benchResults = argGenerator.map(bench)

    // Print benchmark results
    benchResults.foreach( t => {
      val printTime = String.format("Input: %-30s Time(s): %s", t._1.toString, t._2.toString)
      println(printTime)
    })

    spark.stop()
  }

}
