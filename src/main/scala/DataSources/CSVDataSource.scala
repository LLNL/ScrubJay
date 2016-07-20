import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import gov.llnl.scrubjay.datasource._

import java.io.File

import com.github.tototoshi.csv._

package gov.llnl.scrubjay {

  object csvDataSource {

    class CSVDataSource(sc: SparkContext,
                        metaOntology: MetaOntology,
                        metaMap: MetaMap,
                        filename: String) extends OriginalDataSource(metaOntology, metaMap) {

      val reader = CSVReader.open(filename)

      val rdd: RDD[DataRow] = sc.parallelize(reader.allWithHeaders)
    }

    implicit class ScrubJaySession_CSVDataSource(sjs: ScrubJaySession) {

      def createCSVDataSource(metaMap: MetaMap, filename: String): CSVDataSource = {
        new CSVDataSource(sjs.sc, sjs.metaOntology, metaMap, filename)
      }

    }
  }
}
