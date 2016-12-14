package testsuite

import org.apache.spark._
import org.scalactic.source.Position
import org.scalatest.{BeforeAndAfterAll, FunSpec}

trait ScrubJaySpec extends FunSpec with BeforeAndAfterAll {

  var sc: SparkContext = _

  override protected def beforeAll {
    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("ScrubJayTest")
      .setAll(Seq(
        ("spark.serializer", "org.apache.spark.serializer.KryoSerializer"),
        ("spark.kryo.registrator", "scrubjay.registrator.KryoRegistrator")
      ))
    sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
  }

  override protected def afterAll {
    sc.stop()
  }
}


