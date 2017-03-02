package scrubjay.datasource


import scrubjay.metasource._

import scala.pickling._
import scala.pickling.json._
import scala.pickling.Defaults._

import com.roundeights.hasher.Implicits._

abstract class DataSourceID(val dataSourceArgs: Seq[DataSourceID] = Seq())(val otherArgs: Seq[Any] = Seq()) extends Serializable {
  val metaSource: MetaSource
  def realize: ScrubJayRDD
}

object DataSourceID {
  def toJsonString(dsID: DataSourceID): String = dsID.pickle.value
  def fromJsonString(s: String): DataSourceID = functions.unpickle[DataSourceID](JSONPickle(s))
  def toHash(dsID: DataSourceID): String = toJsonString(dsID).sha256.hex
}
