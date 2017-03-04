package testsuite

import org.apache.spark._
import org.scalatest.{BeforeAndAfterAll, FunSpec}

import org.scalactic.source.Position

trait ScrubJaySpec extends FunSpec with BeforeAndAfterAll {

  var sc: SparkContext = _

  override protected def beforeAll {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("ScrubJayTest")
    sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
  }

  override protected def afterAll {
    sc.stop()
  }
}


