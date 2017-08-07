package perftests

trait BenchMain[T] {

  protected val argGenerator: Iterator[T]

  protected def bench(arg: T): (T, Double)

  def main(args: Array[String]): Unit = {

    // Warmup Spark
    GenerateInputs.timeXTemp(100).realize(GenerateInputs.dimensionSpace).collect()

    // Collect benchmark results
    val benchResults = argGenerator.map(bench)

    // Print benchmark results
    benchResults.foreach( t => {
      val printTime = String.format("Input: %-30s Time(s): %s", t._1.toString, t._2.toString)
      println(printTime)
    })
  }

}
