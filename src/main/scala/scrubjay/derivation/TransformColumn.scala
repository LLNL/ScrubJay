package scrubjay.derivation

import org.apache.spark.rdd.RDD
import scrubjay.MetaEntry
import scrubjay.datasource.{DataRow, ScrubJayRDD}
import scrubjay.units.Units

class TransformColumn(dso: Option[ScrubJayRDD], column: String, fn: Units[_] => Units[_], newMetaEntry: MetaEntry)
  extends Transformer(dso) {

  val isValid: Boolean = ds.metaSource.columns contains column

  def derive: ScrubJayRDD = {

    val metaSource = ds.metaSource.withMetaEntries(Map(column -> newMetaEntry), overwrite = true)

    val rdd: RDD[DataRow] = {
      ds.map(row => {
        row ++ Map(column -> fn(row(column)))
      })
    }

    new ScrubJayRDD(rdd, metaSource)
  }
}
