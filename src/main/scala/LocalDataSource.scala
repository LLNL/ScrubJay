import org.apache.spark.rdd.RDD

import scrubjay.datasource._

package scrubjay {

  class LocalDataSource(meta: MetaMap,
                        rdd: RDD[DataRow]) extends DataSource {
    lazy val Meta = meta
    lazy val Data = rdd
  }
}
