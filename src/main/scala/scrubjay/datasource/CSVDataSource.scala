package scrubjay.datasource

import scrubjay._
import scrubjay.meta._
import scrubjay.units._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import au.com.bytecode.opencsv.CSVReader
import java.io.{BufferedWriter, FileReader, FileWriter}

import scala.collection.JavaConversions._

class CSVDataSource(sc: SparkContext,
                    filename: String,
                    providedMetaSource: MetaSource = new EmptyMetaSource,
                    val metaBase: MetaBase)
    extends DataSource {

  // TODO: make CSVs lazily evaluated
  val (header, data) = {
    val reader = new CSVReader(new FileReader(filename))

    val tmpHeader = reader.readNext.map(_.trim)
    val tmpData = reader.readAll.map(row => tmpHeader.zip(row.map(_.trim)).toMap).toList

    (tmpHeader, tmpData)
  }

  val metaSource = providedMetaSource.withColumns(header)

  val rawRdd: RDD[RawDataRow] = sc.parallelize(data)
  lazy val rdd: RDD[DataRow] = Units.rawRDDToUnitsRDD(sc, rawRdd, metaSource.metaEntryMap)
}

object CSVDataSource {
  implicit class ScrubJaySession_CSVDataSource(sjs: ScrubJaySession) {
    def createCSVDataSource(filename: String, metaSource: MetaSource = new EmptyMetaSource): CSVDataSource = {
      new CSVDataSource(sjs.sc, filename, metaSource, sjs.metaBase)
    }
  }

  implicit class DataSource_saveAsCSV(ds: DataSource) {
    def saveAsCSVDataSource(fileName: String,
                            wrapperChar: String = "\"",
                            delimiter: String = ",",
                            noneString: String = "null"): Unit = {

      val header = ds.metaSource.columns
      val csvRdd = ds.rdd.map(row => header.map(wrapperChar + row.getOrElse(_, Identifier(noneString)).raw.toString + wrapperChar).mkString(delimiter))
      val bw = new BufferedWriter(new FileWriter(fileName))

      // Possible TODO: optimize this by collecting a partition at a time
      bw.write(header.map(wrapperChar + _ + wrapperChar).mkString(delimiter))
      bw.newLine()
      csvRdd.toLocalIterator.foreach(rowString => {
        bw.write(rowString)
        bw.newLine()
      })
      bw.close()
    }
  }
}
