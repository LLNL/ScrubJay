package scrubjay.datasource


import scrubjay.metasource._
import com.roundeights.hasher.Implicits._

import scala.pickling.functions

abstract class DataSourceID extends Serializable {
  val metaSource: MetaSource
  def isValid: Boolean
  def realize: ScrubJayRDD
  def asOption: Option[DataSourceID] = {
    if (isValid)
      Some(this)
    else
      None
  }
}

object DataSourceID {

  import scala.pickling.Defaults._
  import scala.pickling.json._

  def toJsonString(dsID: DataSourceID): String = dsID.pickle.value
  def fromJsonString(s: String): DataSourceID = functions.unpickle[DataSourceID](JSONPickle(s))
  def toHash(dsID: DataSourceID): String = toJsonString(dsID).sha256.hex
}
