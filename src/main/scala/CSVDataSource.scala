import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scrubjay.datasource._
import scrubjay.interactive._

import java.io.File

import com.github.tototoshi.csv._

package scrubjay {

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

      /*
      def interactivelyCreateCSVDataSource(filename: String): CSVDataSource = {
        val reader = CSVReader.open(filename)
        val headerReply = readLine("Header [Y/n]? ")
        val hasHeader = headerReply != "n"

        val columnsDataTuple = {
          if (hasHeader) {
            reader.allWithOrderedHeaders
          }
          else {
            val allData = reader.all
            val columns = allData(0).length
            val headers = for (c <- 1 until columns) yield "col" + c.toString

            (headers, allData.map(headers.zip(_).toMap))
          }
        }

        reader.close

        val metaMap = interactiveMetaCreator(sjs.metaOntology, columnsDataTuple._1)

        new CSVDataSource(sjs.sc, metaOntology, metaMap, filename)
      }
      */
    }
  }
}
