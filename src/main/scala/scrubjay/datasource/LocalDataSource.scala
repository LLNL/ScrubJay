package scrubjay.datasource

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.sql.SQLContext._
import scrubjay.metasource._
import scrubjay.units.Units

case class LocalDataSource(rawData: Seq[RawDataRow], metaSourceID: MetaSourceID)
  extends DataSourceID {

  val metaSource: MetaSource = metaSourceID.realize//.withColumns(columns)

  def isValid: Boolean = true

  def realize: ScrubJayRDD = {
    val rawRDD = SparkContext.getOrCreate().parallelize(rawData)
    new ScrubJayRDD(rawRDD, metaSource)
  }
}

object LocalDataSource {
  /*
  def printToStdout(dsID: DataSourceID): Unit = {
    val columns = dsID.metaSource.columns
    val ds: RDD[Row] = dsID.realize.map(row => {
      columns.map(column => {
        val columnVal = row.getOrElse(column, null)
        val columnValValue = if (columnVal == null) null else columnVal.value
        columnValValue
      })
    }).map(Row(_))

    val dataSet = SparkSession.builder().getOrCreate().createDataset(dsID.realize)
    dataSet.toDF().show()
    //SQLContext(SparkContext.getOrCreate())
    //ds.toDF(dsID.metaSource.columns)
  }
  */
}