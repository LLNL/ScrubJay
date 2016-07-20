import gov.llnl.scrubjay._

// Scala
import scala.collection.immutable.Map

// Spark
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

package gov.llnl.scrubjay {

  object datasource {

    type MetaMap = Map[MetaEntry, String]
    type DataRow = Map[String, Any]

    case class MetaDescriptor(title: String, description: String) extends Serializable
    case class MetaEntry(value: MetaDescriptor, units: MetaDescriptor) extends Serializable

    abstract class DataSource(val metaOntology: MetaOntology) extends Serializable {
      val metaMap: MetaMap
      val rdd: RDD[DataRow]

      def containsMeta(meta: List[MetaEntry]): Boolean = {
        meta.forall(metaMap contains _)
      }
    }

    abstract class OriginalDataSource(metaOntology: MetaOntology,
                                      val metaMap: MetaMap) extends DataSource(metaOntology)

    abstract class DerivedDataSource(metaOntology: MetaOntology) extends DataSource(metaOntology) {

      val defined: Boolean
    }
  }
}
