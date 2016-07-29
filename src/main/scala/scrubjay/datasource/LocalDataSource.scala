package scrubjay.datasource

import scrubjay._
import scrubjay.meta._

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class LocalDataSource(metaOntology: MetaBase,
                      metaMap: MetaMap,
                      rddGiven: RDD[DataRow]) extends OriginalDataSource(metaOntology, metaMap) {
  lazy val rdd = rddGiven
}

object LocalDataSource {
  implicit class ScrubJaySession_LocalDataSource(sjs: ScrubJaySession) {
    def createLocalDataSource(metaMap: MetaMap, data: Seq[DataRow]): LocalDataSource = {
      new LocalDataSource(sjs.metaOntology, metaMap, sjs.sc.parallelize(data))
    }
  }
}
