package scrubjay.datasource

import org.apache.spark.rdd.RDD
import scrubjay.metasource.MetaSource

abstract class DataSource extends Serializable {

  val metaSource: MetaSource
  val rdd: RDD[DataRow]

}

object DataSource {

  def empty = new DataSource {
    override val metaSource = MetaSource.empty
    override val rdd = null
  }

}
