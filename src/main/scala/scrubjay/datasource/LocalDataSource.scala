package scrubjay.datasource

import org.apache.spark.SparkContext
import scrubjay.metasource._

case class LocalDataSource(rawData: Seq[RawDataRow], metaSourceID: MetaSourceID)
  extends DataSourceID {

  val metaSource: MetaSource = metaSourceID.realize//.withColumns(columns)

  def isValid: Boolean = true

  def realize: ScrubJayRDD = {
    val rawRDD = SparkContext.getOrCreate().parallelize(rawData)
    new ScrubJayRDD(rawRDD, metaSource)
  }
}

