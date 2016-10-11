package scrubjay.datasource

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import scrubjay._
import scrubjay.meta._
import scrubjay.units._

class LocalDataSource(sc: SparkContext,
                      columns: Seq[String],
                      data: Seq[RawDataRow],
                      providedMetaSource: MetaSource,
                      val metaBase: MetaBase) extends DataSource {

  val metaSource = providedMetaSource.withColumns(columns)
  val rawRdd = sc.parallelize(data)
  lazy val rdd: RDD[DataRow] = Units.rawRDDToUnitsRDD(sc, rawRdd, metaSource.metaEntryMap)
}

object LocalDataSource {
  implicit class ScrubJaySessionImplicits(sjs: ScrubJaySession) {
    def createLocalDataSource(columns: Seq[String], data: Seq[RawDataRow], metaSource: MetaSource): LocalDataSource = {
      new LocalDataSource(sjs.sc, columns, data, metaSource, sjs.metaBase)
    }
  }
}
