package perftests

import scrubjay.datasetid.combination.InterpolationJoin
import scrubjay.util.returnTime

object InterpolationJoinBench extends BenchMain[Long] {

  override protected val argGenerator: Iterator[Long] = 10000L to 30000L by 10000L toIterator

  override protected def bench(numRows: Long): (Long, Double) = {

    val timeTemp = GenerateInputs.timeXTemp(numRows)
    val timeFlops = GenerateInputs.timeXFlops(numRows)

    //timeTemp.realize(GenerateInputs.dimensionSpace).show(false)
    //timeFlops.realize(GenerateInputs.dimensionSpace).show(false)

    lazy val interjoined = InterpolationJoin(timeTemp, timeFlops, 6)
    numRows -> returnTime {
      interjoined.realize(GenerateInputs.dimensionSpace).collect()
    }
  }
}
