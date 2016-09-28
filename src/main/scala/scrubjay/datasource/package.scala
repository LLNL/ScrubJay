package scrubjay

import scrubjay.units._

package object datasource {
  type RawDataRow = Map[String, Any]
  type DataRow = Map[String, Units]
}
