package scrubjay.datasource

import scrubjay._
import scrubjay.meta._
import scrubjay.units._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import au.com.bytecode.opencsv.CSVReader
import java.io.{BufferedWriter, IOException, FileReader, FileWriter}

import scala.collection.JavaConversions._

class CSVDataSource(metaOntology: MetaBase,
                    metaMap: MetaMap,
                    filename: String,
                    sc: SparkContext)
    extends OriginalDataSource(metaOntology, metaMap) {

  val data: List[Map[String, String]] = {
    val reader = new CSVReader(new FileReader(filename))
    val headers = reader.readNext.map(_.trim)
    reader.readAll.map(row => headers.zip(row.map(_.trim)).toMap).toList
  }

  val rdd: RDD[DataRow] = Units.rawRDDToUnitsRDD(sc, sc.parallelize(data), metaMap)
}

object CSVDataSource {
  implicit class ScrubJaySession_CSVDataSource(sjs: ScrubJaySession) {
    def createCSVDataSource(metaMap: MetaMap, filename: String): CSVDataSource = {
      new CSVDataSource(sjs.metaOntology, metaMap, filename, sjs.sc)
    }
  }

  implicit class DataSource_saveAsCSV(ds: DataSource)   {
    def saveAsCSVDataSource(fileName: String): Unit =  {
      val header = ds.metaEntryMap.keys.toSeq
      val csvRdd = ds.rdd.map(row => header.map("\"" + row.getOrElse(_, "null").toString + "\"").mkString(","))
      val bw = new BufferedWriter(new FileWriter(fileName))

      bw.write(header.map("\"" + _ + "\"").mkString(","))
      bw.newLine()
      csvRdd.collect.foreach({ rowString =>
        bw.write(rowString)
        bw.newLine()
      })
      bw.close()
    }
  }
}
