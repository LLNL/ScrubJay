package scrubjay.datasource

import scrubjay.util.niceAttempt
import scrubjay.units.Units
import scrubjay.metasource.MetaSource
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