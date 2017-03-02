package scrubjay.datasource

import scrubjay.metasource._
import scrubjay.util.niceAttempt

import org.apache.spark.rdd.RDD

object LocalDataSource {

  def createLocalDataSource(rawRdd: RDD[RawDataRow],
                            columns: Seq[String],
                            providedMetaSource: MetaSource): Option[ScrubJayRDD] = {
    niceAttempt {
      new ScrubJayRDD(rawRdd, providedMetaSource.withColumns(columns))
    }
  }
}