package scrubjay.datasource

import scrubjay._
import scrubjay.meta._

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import java.io.File

import com.github.tototoshi.csv._

class CSVDataSource(sc: SparkContext,
                    metaOntology: MetaOntology,
                    metaMap: MetaMap,
                    filename: String) 
    extends OriginalDataSource(metaOntology, metaMap) {
  val reader = CSVReader.open(filename)
  val rdd: RDD[DataRow] = sc.parallelize(reader.allWithHeaders)
}

object CSVDataSource {
  // ScrubJaySession method to create a CSVDataSource
  implicit class ScrubJaySession_CSVDataSource(sjs: ScrubJaySession) {
    def createCSVDataSource(metaMap: MetaMap, filename: String): CSVDataSource = {
      new CSVDataSource(sjs.sc, sjs.metaOntology, metaMap, filename)
    }
  }
}
