package scrubjay.datasource

import scrubjay.units._
import scrubjay.metasource._
import java.io._

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class CSVDataSource(csvFileName: String, providedMetaSource: MetaSource)
  extends DataSourceID()(Seq(csvFileName)) {

  // FIXME: with which columns?
  val metaSource: MetaSource = providedMetaSource//.withColumns(header)

  def realize: ScrubJayRDD = {

    import au.com.bytecode.opencsv.CSVReader
    import scala.collection.JavaConversions._
    import java.io._

    val reader = new CSVReader(new FileReader(csvFileName))
    val header = reader.readNext.map(_.trim)
    val rawData = reader.readAll.map(row => header.zip(row.map(_.trim)).toMap).toList

    val rawRDD: RDD[Map[String, Any]] = SparkContext.getOrCreate().parallelize(rawData)
    new ScrubJayRDD(rawRDD, metaSource)
  }
}


object CSVDataSource {

  def apply(csvFileName: String, providedMetaSource: MetaSource): Option[DataSourceID] = {
    Some(new CSVDataSource(csvFileName, providedMetaSource))
  }

  def saveToCSV(dsID: DataSourceID,
                fileName: String,
                wrapperChar: String,
                delimiter: String,
                noneString: String): Unit = {

    val ds = dsID.realize

    val header = dsID.metaSource.columns
    val csvRdd = ds.map(row => header.map(col => wrapperChar +
      row.getOrElse(col, UnorderedDiscrete(noneString)).rawString +
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
