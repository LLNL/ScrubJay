package perftests

//import org.scalameter.api._

import scrubjay.util.argTimeTuple

object InterpolationJoinBench {

  val numRowsRange = 1000L to 10000L by 1000L

  def bench(numRows: Long): Unit = {
    GenerateInputs.timeXTemp(numRows)
  }

  def main(args: Array[String]): Unit = {

    // Warmup Spark
    GenerateInputs.timeXTemp(1)

    // Collect benchmark results
    val benchResults = numRowsRange.map(numRows => argTimeTuple(numRows, bench))

    // Print benchmark results
    benchResults.foreach( t => {
      val printTime = String.format("Input: %-30s Time(s): %s", t._1.toString, t._2.toString)
      println(printTime)
    })
  }
}
