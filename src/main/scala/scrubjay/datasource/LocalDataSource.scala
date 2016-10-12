package scrubjay.datasource

import org.apache.spark.rdd.RDD
import scrubjay.meta._

class LocalDataSource(rawRdd: RDD[RawDataRow],
                      columns: Seq[String],
                      providedMetaSource: MetaSource)
  extends DataSource(rawRdd, columns, providedMetaSource)
