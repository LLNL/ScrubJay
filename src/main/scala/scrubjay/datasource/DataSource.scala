package scrubjay.datasource

import scrubjay.meta._

// Spark
import org.apache.spark.rdd.RDD

abstract class DataSource(val metaOntology: MetaBase) extends Serializable {
  val metaMap: MetaMap
  val rdd: RDD[DataRow]

  def metaForColumn(column: String): Option[MetaEntry] = {
    metaMap.get(column)
  }

  def columnForMeta(metaEntry: MetaEntry): Option[String] = {
    metaMap.map(_.swap).get(metaEntry)
  }

  def containsMeta(meta: List[MetaEntry]): Boolean = {
    meta.forall(metaMap.values.toSet.contains)
  }
}

abstract class OriginalDataSource(metaOntology: MetaBase,
                                  val metaMap: MetaMap) extends DataSource(metaOntology)

abstract class DerivedDataSource(metaOntology: MetaBase) extends DataSource(metaOntology) {

  val defined: Boolean
}
