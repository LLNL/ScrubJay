package scrubjay.datasource

import scrubjay.metasource._

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class CaliperKeyValueDataSource(ckvFileName: String, metaSourceID: MetaSourceID) extends DataSourceID {

  override val metaSource: MetaSource = metaSourceID.realize

  override def isValid: Boolean = true

  override def realize: ScrubJayRDD = {
    import au.com.bytecode.opencsv.CSVReader
    import scala.collection.JavaConversions._
    import java.io._

    val reader = new CSVReader(new FileReader(ckvFileName))
    val rawData = reader.readAll.map(row =>
      row.map(_.split("=") match {
        case Array(key, value) => (key, value)
      }).toMap)

    rawData.take(10).foreach(println)

    val rawRDD: RDD[Map[String, Any]] = SparkContext.getOrCreate().parallelize(rawData)
    new ScrubJayRDD(rawRDD, metaSource)
  }
}
