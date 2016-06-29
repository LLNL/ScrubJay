import scrubjay._

// Scala
import scala.collection.immutable.Map

// Spark
import org.apache.spark.rdd.RDD

package scrubjay {

  object datasource {

    type MetaMap = Map[MetaEntry, String]
    type DataRow = Map[String, Any]

    case class MetaDescriptor(title: String, description: String) extends Serializable
    case class MetaEntry(value: MetaDescriptor, units: MetaDescriptor) extends Serializable

    abstract class DataSource extends Serializable {
      val Meta: MetaMap
      val Data: RDD[DataRow]
      val MetaDefinitions: MetaDefinitionMaker = new MetaDefinitionMaker
    }

    abstract class DerivedDataSource(val datasources: DataSource*) extends DataSource {

      val RequiredMetaEntries: List[List[MetaEntry]]
      val DerivedMetaEntries: MetaMap

      lazy val Defined: Boolean = {
        // Check that each datasource contains all required meta entries
        datasources.zip(RequiredMetaEntries).forall({ 
          case (ds, reqs) => reqs.forall(req => {
            ds.Meta contains req
          })
        })
      }

      lazy val Meta: MetaMap = {
        // Union of meta entries of all datasources and newly derived ones
        datasources.map(ds => ds.Meta).reduce((m1, m2) => m1 ++ m2) ++ DerivedMetaEntries
      }

    }
  }
}
