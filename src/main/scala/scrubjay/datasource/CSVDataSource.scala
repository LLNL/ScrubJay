package scrubjay.datasource

import scrubjay.units._
import scrubjay.util.niceAttempt
import scrubjay.metasource.MetaSource

import java.io._

import org.apache.spark.rdd.RDD


abstract class CSVDataSource extends DataSource {
  val fileName: String
}

object CSVDataSource {

  def createCSVDataSource(rawRdd: RDD[RawDataRow],
                          header: Seq[String],
                          csvFileName: String,
                          providedMetaSource: MetaSource): Option[CSVDataSource] = {

    niceAttempt {

      new CSVDataSource {
        override val fileName = csvFileName
        override lazy val metaSource = providedMetaSource.withColumns(header)
        override lazy val rdd = Units.rawRDDToUnitsRDD(rawRdd, metaSource.metaEntryMap)
      }

    }
  }

  def saveToCSV(ds: DataSource,
                fileName: String,
                wrapperChar: String,
                delimiter: String,
                noneString: String): Unit = {

    val header = ds.metaSource.columns
    val csvRdd = ds.rdd.map(row => header.map(col => wrapperChar +
      row.getOrElse(col, UnorderedDiscrete(noneString)).value.toString +
      wrapperChar).mkString(delimiter))
    val bw = new BufferedWriter(new FileWriter(fileName))

    // TODO (possibly): optimize this by collecting a partition at a time
    bw.write(header.map(wrapperChar + _ + wrapperChar).mkString(delimiter))
    bw.newLine()
    csvRdd.collect.foreach(rowString => {
      bw.write(rowString)
      bw.newLine()
    })
    bw.close()
  }
}
