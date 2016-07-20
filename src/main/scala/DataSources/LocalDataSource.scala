import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scrubjay.datasource._

package scrubjay {

  object localDataSource {
    class LocalDataSource(metaOntology: MetaOntology,
                          metaMap: MetaMap,
                          rddGiven: RDD[DataRow]) extends OriginalDataSource(metaOntology, metaMap) {
      lazy val rdd = rddGiven
    }

    implicit class ScrubJaySession_LocalDataSource(sjs: ScrubJaySession) {
      def createLocalDataSource(metaMap: MetaMap, data: Seq[DataRow]): LocalDataSource = {
        new LocalDataSource(sjs.metaOntology, metaMap, sjs.sc.parallelize(data))
      }
    }
  }
}
