// Copyright 2018 Lawrence Livermore National Security, LLC and other
// ScrubJay Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

package scrubjay.benchmark

import org.apache.spark.sql.SparkSession
import scrubjay.datasetid.combination.NaturalJoin
import scrubjay.util.returnTime

class NaturalJoinBench(repeats: Long = 10,
                             startRows: Long = 10000L,
                             endRows: Long = 50000L,
                             stepRows: Long = 10000L)
  extends BenchMark[Long] {

  override protected val argGenerator: Iterator[Long] = (startRows to endRows by stepRows).toIterator

  override protected def bench(numRows: Long): Seq[(Long, Long, Double)] = {

    val nodeTemp = GenerateInputs.nodeXFlops(numRows)
    val nodeFlops = GenerateInputs.nodeXTemperature(numRows)

    val results = for (r <- 1L to repeats) yield {
      lazy val naturalJoined = NaturalJoin(nodeTemp, nodeFlops)
      (
        r,
        numRows,
        returnTime(naturalJoined.realize.collect())
      )
    }

    SparkSession.builder().getOrCreate().sqlContext.clearCache()

    results
  }
}

