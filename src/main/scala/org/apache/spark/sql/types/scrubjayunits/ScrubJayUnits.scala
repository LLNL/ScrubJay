package org.apache.spark.sql.types.scrubjayunits

import org.apache.spark.sql.types.DataType

trait ScrubJayUDTObject {
  def sqlType: DataType
}

abstract class ScrubJayUnits extends Serializable {
}
