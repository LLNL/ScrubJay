package scrubjay.benchmark

import org.apache.spark.sql.SparkSession
import scrubjay.datasetid.combination.InterpolationJoin
import scrubjay.util.returnTime

class InterpolationJoinBench(repeats: Long = 10,
                             startRows: Long = 10000L,
                             endRows: Long = 50000L,
                             stepRows: Long = 10000L)
  extends BenchMark[Long] {

  override protected val argGenerator: Iterator[Long] = (startRows to endRows by stepRows).toIterator

  override protected def bench(numRows: Long): Seq[(Long, Long, Double)] = {

    val timeTemp = GenerateInputs.timeXTemp(numRows)
    val timeFlops = GenerateInputs.timeXFlops(numRows)

    val results = for (r <- 1L to repeats) yield {
      lazy val interjoined = InterpolationJoin(timeTemp, timeFlops, 6)
      (
        r,
        numRows,
        returnTime(interjoined.realize(GenerateInputs.dimensionSpace).collect())
      )
    }

    SparkSession.builder().getOrCreate().sqlContext.clearCache()

    results
  }
}
