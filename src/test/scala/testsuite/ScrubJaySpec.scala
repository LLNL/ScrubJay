package testsuite

import org.apache.spark._
import org.scalactic.source.Position
import org.scalatest.{BeforeAndAfterAll, FunSpec}

trait ScrubJaySpec extends FunSpec with BeforeAndAfterAll {

  var sc: SparkContext = _

  override protected def beforeAll {
    sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("ScrubJayTest"))
    sc.setLogLevel("WARN")
  }

  override protected def afterAll {
    sc.stop()
  }
}


