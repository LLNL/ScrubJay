package scrubjay.datasource

import scrubjay._
import scrubjay.meta._
import scrubjay.units._

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect._

class LocalDataSource(metaOntology: MetaBase,
                      metaMap: MetaMap,
                      rddGiven: RDD[RawDataRow],
                      sc: SparkContext) extends OriginalDataSource(metaOntology, metaMap) {
  lazy val rdd: RDD[DataRow] = Units.rawRDDToUnitsRDD(sc, rddGiven, metaMap)
}

object LocalDataSource {
  implicit class ScrubJaySession_LocalDataSource(sjs: ScrubJaySession) {
    def createLocalDataSource(metaMap: MetaMap, data: Seq[RawDataRow]): LocalDataSource = {
      new LocalDataSource(sjs.metaOntology, metaMap, sjs.sc.parallelize(data), sjs.sc)
    }
  }
}
