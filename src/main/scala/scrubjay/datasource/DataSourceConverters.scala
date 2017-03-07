package scrubjay.datasource

import scrubjay.metasource._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object DataSourceConverters {
  val spark: SparkSession = SparkSession.builder().getOrCreate()

  def rowRDDFromDataSourceID(dsID: DataSourceID): RDD[Row] = {
    val columns = dsID.metaSource.columns
    dsID.realize.map(row => {
      columns.map(column => {
        val columnVal = row.getOrElse(column, null)
        val columnValValue = if (columnVal == null) null else columnVal.rawString
        columnValValue
      })
    }).map(stringSeq => Row(stringSeq:_*))
  }

  def dataFrameFromDataSourceID(dsID: DataSourceID): DataFrame = {
    val columns = dsID.metaSource.columns
    val fields = columns.map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)
    spark.createDataFrame(rowRDDFromDataSourceID(dsID), schema)
  }

  def dataSetFromDataSourceID(dsID: DataSourceID): Dataset[Row] = {
    ??? //spark.createDataset(rowRDDFromDataSourceID(dsID))
  }
}
