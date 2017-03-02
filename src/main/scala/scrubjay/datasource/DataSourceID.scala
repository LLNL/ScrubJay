package scrubjay.datasource

import scala.pickling._
import scala.pickling.json._
import scala.pickling.Defaults._

case class DataSourceID(creator: String)(dataSourceArgs: Seq[DataSourceID] = Seq())(otherArgs: Seq[Any] = Seq())

object DataSourceID {
  def toJsonString(d: DataSourceID): String = d.pickle.value
  def fromJsonString(s: String): DataSourceID = functions.unpickle[DataSourceID](JSONPickle(s))
}
