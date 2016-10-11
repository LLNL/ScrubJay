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

  val header = {
    val reader = new CSVReader(new FileReader(filename))
    reader.readNext.map(_.trim)
  }

  lazy val data = {
    val reader = new CSVReader(new FileReader(filename))
    reader.readNext // skip header
    reader.readAll.map(row => header.zip(row.map(_.trim)).toMap).toList
  }

  val metaSource = providedMetaSource.withColumns(header)

  lazy val rawRdd: RDD[RawDataRow] = sc.parallelize(data)
  lazy val rdd: RDD[DataRow] = Units.rawRDDToUnitsRDD(sc, rawRdd, metaSource.metaEntryMap)
}

object CSVDataSource {
  implicit class ScrubJaySessionImplicits(sjs: ScrubJaySession) {
    def createCSVDataSource(filename: String, metaSource: MetaSource = new EmptyMetaSource): CSVDataSource = {
      new CSVDataSource(sjs.sc, filename, metaSource, sjs.metaBase)
    }
  }

  implicit class DataSourceImplicits(ds: DataSource) {
    def saveAsCSVDataSource(fileName: String,
                            wrapperChar: String = "\"",
                            delimiter: String = ",",
                            noneString: String = "null"): Unit = {

      val header = ds.metaSource.columns
      val csvRdd = ds.rdd.map(row => header.map(wrapperChar + row.getOrElse(_, Identifier(noneString)).value.toString + wrapperChar).mkString(delimiter))
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
