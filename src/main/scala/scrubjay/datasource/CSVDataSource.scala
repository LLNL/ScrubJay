package scrubjay.datasource

import scrubjay.meta._
import scrubjay.units._
import org.apache.spark.rdd.RDD
import java.io.{BufferedWriter, FileWriter}


class CSVDataSource(rawRdd: RDD[RawDataRow],
                    columns: Seq[String],
                    providedMetaSource: MetaSource)
    extends DataSource(rawRdd, columns, providedMetaSource) {
}

object CSVDataSource {

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
