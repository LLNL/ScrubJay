package scrubjay

import scrubjay.meta._

package object datasource {
  type DataRow = Map[String, Any]
  type MetaMap = Map[MetaEntry, String]
}
