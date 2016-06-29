import scrubjay._

// Scala
import scala.collection.immutable.Map

// Spark
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

package scrubjay {

  object datasource {

    type MetaMap = Map[MetaEntry, String]
    type DataRow = Map[String, Any]

    case class MetaDescriptor(title: String, description: String) extends Serializable
    case class MetaEntry(value: MetaDescriptor, units: MetaDescriptor) extends Serializable

    abstract class DataSource(val metaOntology: MetaOntology) extends Serializable {
      val metaMap: MetaMap
      val rdd: RDD[DataRow]
    }

    abstract class OriginalDataSource(metaOntology: MetaOntology,
                                      val metaMap: MetaMap) extends DataSource(metaOntology)

    abstract class DerivedDataSource(metaOntology: MetaOntology,
                                     val datasources: DataSource*) extends DataSource(metaOntology) {

      val requiredMetaEntries: List[List[MetaEntry]]
      val derivedMetaEntries: MetaMap

      lazy val defined: Boolean = {
        // Check that each datasource contains all required meta entries
        datasources.zip(requiredMetaEntries).forall({ 
          case (ds, reqs) => reqs.forall(req => {
            ds.metaMap contains req
          })
        })
      }

      lazy val metaMap: MetaMap = {
        // Union of meta entries of all datasources and newly derived ones
        datasources.map(ds => ds.metaMap).reduce((m1, m2) => m1 ++ m2) ++ derivedMetaEntries
      }

    }
  }
}
