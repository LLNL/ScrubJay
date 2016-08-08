package scrubjay

import scrubjay.meta._
import scrubjay.units._

package object datasource {
  type RawDataRow = Map[String, Any]
  type DataRow = Map[String, Units]
  type MetaMap = Map[String, MetaEntry]
}
