import collection.immutable.HashSet

import scrubjay.datasource._

package scrubjay {

  abstract class MetaOntologyQueryable extends Serializable {

    var ontologyMap: Map[Int,MetaDescriptor] = Map.empty

    def addDefinition(meta_desc: MetaDescriptor): MetaDescriptor = {
      ontologyMap = ontologyMap ++ Map(meta_desc.hashCode -> meta_desc)
      meta_desc
    }

    def lookup(i: Int): MetaDescriptor = ontologyMap(i)

  }

  class MetaOntology extends MetaOntologyQueryable {

    // Values
    final val VALUE_JOB_ID     = addDefinition(MetaDescriptor("Job ID", "Unique identifier for a job submitted via SLURM"))
    final val VALUE_JOB_NAME   = addDefinition(MetaDescriptor("Job Name", "Name of the job submitted via SLURM"))
    final val VALUE_START_TIME = addDefinition(MetaDescriptor("Start Time", "An instantaneous point in time"))
    final val VALUE_DURATION   = addDefinition(MetaDescriptor("Time Duration", "A quantity of time"))
    final val VALUE_NODE       = addDefinition(MetaDescriptor("Node", "An individual node in an HPC cluster"))
    final val VALUE_NODE_LIST  = addDefinition(MetaDescriptor("Node List", "A list of nodes in an HPC cluster"))
    final val VALUE_NUM_NODES  = addDefinition(MetaDescriptor("Node", "An individual node in an HPC cluster"))
    final val VALUE_PARTITION  = addDefinition(MetaDescriptor("Parition", "A node partition in an HPC cluster"))
    final val VALUE_STATE      = addDefinition(MetaDescriptor("State", "The state of a job (completed, cancelled, etc.)"))
    final val VALUE_USER_NAME  = addDefinition(MetaDescriptor("User Name", "A user's name as it appears to the OS"))

    // Units
    final val UNITS_ID         = addDefinition(MetaDescriptor("Identifier", "Categorical value that describes an individual element"))
    final val UNITS_TIME       = addDefinition(MetaDescriptor("Human Time", "Time represented by human clocks/calendars"))
    final val UNITS_SECONDS    = addDefinition(MetaDescriptor("Seconds", "Seconds as described by human clocks"))
    final val UNITS_QUANTITY   = addDefinition(MetaDescriptor("Quantity", "An amount of some instance"))
    final val UNITS_ID_LIST    = addDefinition(MetaDescriptor("ID List", "A list of categorical values that describe elements"))

  }
}
