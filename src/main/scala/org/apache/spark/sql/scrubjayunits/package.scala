package org.apache.spark.sql

import org.apache.spark.sql.types.DataType
import org.json4s.JsonAST.JValue

package object scrubjayunits {
  implicit class DataTypeExposePrivates(dt: DataType) {
    def getJValue: JValue = dt.jsonValue
  }
}
