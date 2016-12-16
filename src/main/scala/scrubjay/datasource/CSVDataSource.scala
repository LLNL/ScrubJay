package scrubjay.datasource

import scrubjay.units._
import scrubjay.util.niceAttempt
import scrubjay.metasource.MetaSource

import java.io._

import org.apache.spark.rdd.RDD


trait CSVDataSource {
  val fileName: String
}

object CSVDataSource {

  def createCSVDataSource(rawRdd: RDD[RawDataRow],
                          header: Seq[String],
                          csvFileName: String,
                          providedMetaSource: MetaSource): Option[ScrubJayRDD with CSVDataSource] = {

    niceAttempt {

      val newMetaSource = providedMetaSource.withColumns(header)

      new ScrubJayRDD(rawRdd, newMetaSource) with CSVDataSource {
        override val fileName: String = csvFileName
      }

    }
  }

  def saveToCSV(ds: ScrubJayRDD,
                fileName: String,
                wrapperChar: String,
                delimiter: String,
                noneString: String): Unit = {

    val header = ds.metaSource.columns
    val csvRdd = ds.map(row => header.map(col => wrapperChar +
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
