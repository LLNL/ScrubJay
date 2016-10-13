package scrubjay.datasource

import scrubjay.util.niceAttempt
import scrubjay.units.Units
import scrubjay.metasource.MetaSource
import org.apache.spark.rdd.RDD

abstract class LocalDataSource extends DataSource

object LocalDataSource {

  def createLocalDataSource(rawRdd: RDD[RawDataRow],
                            columns: Seq[String],
                            providedMetaSource: MetaSource): Option[LocalDataSource] = {
    niceAttempt {
      new LocalDataSource {
        override lazy val metaSource = providedMetaSource.withColumns(columns)
        override lazy val rdd = Units.rawRDDToUnitsRDD(rawRdd, metaSource.metaEntryMap)
      }
    }

  }
}