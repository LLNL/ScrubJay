package scrubjay.datasource

import scrubjay._
import scrubjay.meta._
import scrubjay.units._

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import com.github.tototoshi.csv._

class CSVDataSource(metaOntology: MetaBase,
                    metaMap: MetaMap,
                    filename: String,
                    sc: SparkContext)
    extends OriginalDataSource(metaOntology, metaMap) {
  val reader = CSVReader.open(filename)
  val rdd: RDD[DataRow] = Units.rawRDDToUnitsRDD(sc, sc.parallelize(reader.allWithHeaders), metaMap)
}

object CSVDataSource {
  implicit class ScrubJaySession_CSVDataSource(sjs: ScrubJaySession) {
    def createCSVDataSource(metaMap: MetaMap, filename: String): CSVDataSource = {
      new CSVDataSource(sjs.metaOntology, metaMap, filename, sjs.sc)
    }
  }
}
