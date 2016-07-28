package scrubjay.meta

import collection.immutable.HashSet

abstract class MetaDescriptor(title: String, description: String) extends Serializable

case class MetaMeaning(title: String, description: String) extends MetaDescriptor(title, description)
case class MetaDimension(title: String, description: String) extends MetaDescriptor(title, description)
case class MetaDomain(title: String, description: String) extends MetaDescriptor(title, description)
case class MetaUnits(title: String, description: String) extends MetaDescriptor(title, description)

case class MetaEntry(meaning: MetaMeaning, 
                     dimension: MetaDimension, 
                     domain: MetaDomain, 
                     units: MetaUnits) extends Serializable

abstract class MetaOntologyQueryable extends Serializable {

  var ontologyMap: Map[Int,MetaDescriptor] = Map.empty

  def addDefinition(metaDescriptor: MetaDescriptor): MetaDescriptor = {
    ontologyMap = ontologyMap ++ Map(metaDescriptor.hashCode -> meta_desc)
    meta_desc
  }

  def lookup(i: Int): MetaDescriptor = ontologyMap(i)

}

class MetaOntology extends MetaOntologyQueryable {

  // Values
  final val VALUE_JOB_ID     = addDefinition(MetaDescriptor("Job ID", "Unique identifier for a job submitted via SLURM"))
  final val VALUE_JOB_NAME   = addDefinition(MetaDescriptor("Job Name", "Name of the job submitted via SLURM"))
  final val VALUE_START_TIME = addDefinition(MetaDescriptor("Start Time", "An instantaneous starting point in time"))
  final val VALUE_END_TIME   = addDefinition(MetaDescriptor("End Time", "An instantaneous ending point in time"))
  final val VALUE_DURATION   = addDefinition(MetaDescriptor("Time Duration", "A quantity of time"))
  final val VALUE_RACK       = addDefinition(MetaDescriptor("Rack", "An individual rack in an HPC cluster"))
  final val VALUE_NODE       = addDefinition(MetaDescriptor("Node", "An individual node in an HPC cluster"))
  final val VALUE_RACK_COLUMN= addDefinition(MetaDescriptor("Rack Column", "A column of nodes in an HPC rack"))
  final val VALUE_RACK_ROW   = addDefinition(MetaDescriptor("Rack Row", "A row of nodes in an HPC rack"))
  final val VALUE_NODE_LIST  = addDefinition(MetaDescriptor("Node List", "A list of nodes in an HPC cluster"))
  final val VALUE_NUM_NODES  = addDefinition(MetaDescriptor("Node", "An individual node in an HPC cluster"))
  final val VALUE_PARTITION  = addDefinition(MetaDescriptor("Parition", "A node partition in an HPC cluster"))
  final val VALUE_STATE      = addDefinition(MetaDescriptor("State", "The state of a job (completed, cancelled, etc.)"))
  final val VALUE_USER_NAME  = addDefinition(MetaDescriptor("User Name", "A user's name as it appears to the OS"))
  final val VALUE_TIME_STAMP  = addDefinition(MetaDescriptor("Time Stamp", "An instantaneous point in time"))
  final val VALUE_TIME_RANGE  = addDefinition(MetaDescriptor("Time Range", "A range of time"))

  // Units
  final val UNITS_ID         = addDefinition(MetaDescriptor("Identifier", "Categorical value that describes an individual element"))
  final val UNITS_TIMESTAMP  = addDefinition(MetaDescriptor("Datetimestamp", "Time represented by human clocks/calendars"))
  final val UNITS_EPOCH      = addDefinition(MetaDescriptor("Seconds Since Epoch", "Number of seconds since the epoch"))
  final val UNITS_SECONDS    = addDefinition(MetaDescriptor("Seconds", "Seconds as described by human clocks"))
  final val UNITS_QUANTITY   = addDefinition(MetaDescriptor("Quantity", "An amount of some instance"))
  final val UNITS_ID_LIST    = addDefinition(MetaDescriptor("ID List", "A list of categorical values that describe elements"))
  final val UNITS_TUPLE_TIMESTAMP_TIMESTAMP    = addDefinition(MetaDescriptor("Time Range", "A tuple of timestamps representing a range"))

}
