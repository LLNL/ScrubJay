// Copyright 2018 Lawrence Livermore National Security, LLC and other
// ScrubJay Project Developers. See the top-level COPYRIGHT file for details.
//
// SPDX-License-Identifier: (Apache-2.0 OR MIT)

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
