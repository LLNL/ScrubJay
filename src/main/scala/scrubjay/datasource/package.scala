package scrubjay

package object datasource {

  type MetaMap = Map[MetaEntry, String]
  type DataRow = Map[String, Any]

  case class MetaDescriptor(title: String, description: String) extends Serializable
  case class MetaEntry(value: MetaDescriptor, units: MetaDescriptor) extends Serializable

}
