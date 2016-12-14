package scrubjay.derivation

import org.apache.spark.rdd.RDD
import scrubjay.{DataSource, MetaEntry}
import scrubjay.datasource.DataRow
import scrubjay.units.Units

class TransformColumn(dso: Option[DataSource], column: String, fn: Units[_] => Units[_], newMetaEntry: MetaEntry)
  extends Transformer(dso) {

  val isValid = ds.metaSource.columns contains column

  def derive: DataSource = new DataSource {

    override lazy val metaSource = ds.metaSource.withMetaEntries(Map(column -> newMetaEntry))
      .withoutColumns(Seq(column))

    override lazy val rdd: RDD[DataRow] = {

      ds.rdd.map(row => {
        val newVal = fn(row(column))
        row.filterNot(_._1 == column) ++ Map(column -> newVal)
      })
    }
  }
}
