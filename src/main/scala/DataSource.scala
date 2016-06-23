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

    /*
     * Meta attribute ontology
    */

    var MetaDescriptorLookup: Map[Int, MetaDescriptor] = null

    def DefineMeta(meta_desc: MetaDescriptor): MetaDescriptor = {
      if (MetaDescriptorLookup == null) {
        MetaDescriptorLookup = Map(meta_desc.hashCode -> meta_desc)
      }
      else {
        MetaDescriptorLookup = MetaDescriptorLookup ++ Map(meta_desc.hashCode -> meta_desc)
      }
      meta_desc
    }

    // Values
    final val META_VALUE_JOB_ID     : MetaDescriptor = DefineMeta(MetaDescriptor("Job ID", "Unique identifier for a job submitted via SLURM"))
    final val META_VALUE_START_TIME : MetaDescriptor = DefineMeta(MetaDescriptor("Start Time", "An instantaneous point in time"))
    final val META_VALUE_DURATION   : MetaDescriptor = DefineMeta(MetaDescriptor("Time Duration", "A quantity of time"))
    final val META_VALUE_NODE       : MetaDescriptor = DefineMeta(MetaDescriptor("Node", "An individual node in an HPC cluster"))
    final val META_VALUE_NODE_LIST  : MetaDescriptor = DefineMeta(MetaDescriptor("Node List", "A list of nodes in an HPC cluster"))

    // Units
    final val META_UNITS_ID         : MetaDescriptor = DefineMeta(MetaDescriptor("Identifier", "Categorical value that describes an individual element"))
    final val META_UNITS_TIME       : MetaDescriptor = DefineMeta(MetaDescriptor("Human Time", "Time represented by human clocks/calendars"))
    final val META_UNITS_SECONDS    : MetaDescriptor = DefineMeta(MetaDescriptor("Seconds", "Seconds as described by human clocks"))
    final val META_UNITS_ID_LIST    : MetaDescriptor = DefineMeta(MetaDescriptor("ID List", "A list of categorical values that describe elements"))

    
  }
}
