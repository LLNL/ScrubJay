package scrubjay.datasource

import org.apache.spark.SparkContext
import scrubjay.metasource._

class LocalDataSource(rawData: Seq[RawDataRow],
                      columns: Seq[String],
                      providedMetaSource: MetaSource) extends DataSourceID()(Seq(rawData, columns)) {

  val metaSource: MetaSource = providedMetaSource.withColumns(columns)

  def realize: ScrubJayRDD = {
    val rawRDD = SparkContext.getOrCreate().parallelize(rawData)
    new ScrubJayRDD(rawRDD, metaSource)
  }
}

object LocalDataSource {
  def apply(rawData: Seq[RawDataRow],
            columns: Seq[String],
            providedMetaSource: MetaSource): Option[LocalDataSource] = {
    Some(new LocalDataSource(rawData, columns, providedMetaSource))
  }
}