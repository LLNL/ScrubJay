package testsuite

import scrubjay._
import org.apache.spark._
import org.scalatest._
import org.scalactic.source.Position
import scrubjay.datasource.DataSource


class LocalDataSourceSpec extends FunSpec with BeforeAndAfterAll {

  var sc: SparkContext = _

  override protected def beforeAll {
    sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("ScrubJayTest"))
  }

  override protected def afterAll {
    sc.stop()
  }

  describe("LocalDataSource") {

    lazy val jobQueue = sc.createLocalDataSource(jobQueueRawData, jobQueueMeta.keySet.toSeq, new MetaSource(jobQueueMeta))
    lazy val cabLayout = sc.createLocalDataSource(clusterLayoutRawData, clusterLayoutMeta.keySet.toSeq, new MetaSource(clusterLayoutMeta))

    describe("Creation") {

      // Create local data sources

      describe("Locally generated job queue data") {
        it("should be defined") {
          assert(jobQueue.isDefined)
        }
        it("should match ground truth") {
          assert(jobQueue.get.rdd.collect.toSet == trueJobQueue)
        }
      }


      describe("Locally generated cab layout data") {
        it("should be defined") {
          assert(cabLayout.isDefined)
        }
        it("should match ground truth") {
          assert(cabLayout.get.rdd.collect.toSet == trueCabLayout)
        }
      }
    }

    describe("Derivations") {

      // Time span
      lazy val jobQueueSpan = jobQueue.get.deriveTimeSpan

      describe("Job queue with derived time span") {
        it("should be defined") {
          assert(jobQueueSpan.isDefined)
        }
        it("should match ground truth") {
          assert(jobQueueSpan.get.rdd.collect.toSet == trueJobQueueSpan)
        }
      }

      // Expanded node list
      lazy val jobQueueSpanExploded = jobQueueSpan.get.deriveExplodeList(Seq("nodelist"))

      describe("Job queue with derived time span AND exploded node list") {
        it("should be defined") {
          assert(jobQueueSpanExploded.isDefined)
        }
        it("should match ground truth") {
          assert(jobQueueSpanExploded.get.rdd.collect.toSet == trueJobQueueSpanExploded)
        }
      }

      // Joined with cab layout
      lazy val jobQueueSpanExpandedJoined = jobQueueSpanExploded.get.deriveNaturalJoin(cabLayout)

      describe("Job queue with derived time span AND exploded node list AND joined with cab layout") {
        it("should be defined") {
          assert(jobQueueSpanExpandedJoined.isDefined)
        }
        it("should match ground truth") {
          assert(jobQueueSpanExpandedJoined.get.rdd.collect.toSet == trueJobQueueSpanExplodedJoined)
        }
      }
    }
  }
}
