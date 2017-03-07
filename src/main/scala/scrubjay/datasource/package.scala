package scrubjay

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import scrubjay.units._

import scala.language.implicitConversions

package object datasource {

  type RawDataRow = Map[String, Any]
  type DataRow = Map[String, Units[_]]

  case class ParsedRDD(rdd: RDD[DataRow])
  case class RawRDD(rdd: RDD[RawDataRow])

  implicit def rawRDD2Class(rdd: RDD[RawDataRow]): RawRDD = RawRDD(rdd)
  implicit def parsedRDD2Class(rdd: RDD[DataRow]): ParsedRDD = ParsedRDD(rdd)
  implicit def rawClass2RawRDD(rrdd: RawRDD): RDD[RawDataRow] = rrdd.rdd
  implicit def parsedClass2ParsedRDD(prdd: ParsedRDD): RDD[DataRow] = prdd.rdd

  implicit class ImplicitConverters(dsID: DataSourceID) {
    def toRowRDD: RDD[Row] = DataSourceConverters.rowRDDFromDataSourceID(dsID)
    def toDataFrame: DataFrame = DataSourceConverters.dataFrameFromDataSourceID(dsID)
    def toDataSet: Dataset[Row] = DataSourceConverters.dataSetFromDataSourceID(dsID)
  }
}
