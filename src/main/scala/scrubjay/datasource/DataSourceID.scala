package scrubjay.datasource



import scrubjay.metasource._
import com.roundeights.hasher.Implicits._
import scrubjay.combination.{InterpolationJoin, NaturalJoin}
import scrubjay.transformation.{ExplodeContinuousRange, ExplodeDiscreteRange}


abstract class DataSourceID(inChildren: DataSourceID*) extends Serializable {
  val metaSource: MetaSource
  val children: Seq[DataSourceID] = inChildren

  def isValid: Boolean
  def realize: ScrubJayRDD
  def asOption: Option[DataSourceID] = {
    if (isValid)
      Some(this)
    else
      None
  }
  def describe(): Unit = {
    println(DataSourceID.toJsonString(this))
    println(this)
    this.toDataFrame.show(false)
  }
}

object DataSourceID {

  import scala.pickling.functions
  import scala.pickling.Defaults._
  import scala.pickling.json._

  def toJsonString(dsID: DataSourceID): String = dsID.pickle.value
  def fromJsonString(s: String): DataSourceID = functions.unpickle[DataSourceID](JSONPickle(s))
  def toHash(dsID: DataSourceID): String = "h" + toJsonString(dsID).sha256.hex

  protected def toNodeEdgeTuple(dsID: DataSourceID, parentName: Option[String] = None): (Seq[String], Seq[String]) = {

    val hash: String = toHash(dsID)

    // Graph node
    val style = dsID match {

      // Combined data sources
      case _: NaturalJoin => "style=filled, fillcolor=\"forestgreen\", label=\"NaturalJoin\""
      case _: InterpolationJoin => "style=filled, fillcolor=\"lime\", label=\"InterpolationJoin\""

      // Transformed data sources
      case _: ExplodeDiscreteRange => "style=filled, fillcolor=\"deepskyblue\", label=\"ExplodeDiscrete\""
      case _: ExplodeContinuousRange => "style=filled, fillcolor=\"lightskyblue\", label=\"ExplodeContinuous\""

      // Original data sources
      case _: CSVDataSource => "style=filled, fillcolor=\"darkorange\", label=\"CSVDataSource\""
      case _: CassandraDataSource => "style=filled, fillcolor=\"darkorange\", label=\"CassandraDataSource\""
      case _: LocalDataSource => "style=filled, fillcolor=\"darkorange\", label=\"LocalDataSource\""

      // Unknown
      case _  => "label='unknown'"
    }
    val node: String = hash + " [" + style + "]"

    // Graph edge
    val edge: Seq[String] = {
      if (parentName.isDefined)
        Seq(parentName.get + " -> " + hash + " [penwidth=2]")
      else
        Seq()
    }

    val (childNodes: Seq[String], childEdges: Seq[String]) = dsID.children
      .map(toNodeEdgeTuple(_, Some(hash)))
      .fold((Seq.empty, Seq.empty))((a, b) => (a._1 ++ b._1, a._2 ++ b._2))

    (node +: childNodes, edge ++ childEdges)
  }

  def toDotString(dsID: DataSourceID): String = {

    val (nodes, edges) = toNodeEdgeTuple(dsID)

    val header = "digraph {"
    val nodeSection = nodes.map("\t" + _).mkString("\n")
    val edgeSection = edges.map("\t\t" + _).mkString("\n")
    val footer = "}"

    Seq(header, nodeSection, edgeSection, footer).mkString("\n")
  }

  protected def saveStringToFile(text: String, filename: String): Unit = {
    import java.io.{BufferedWriter, File, FileWriter}

    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(text)
    bw.close()
  }

  def saveToJson(dsID: DataSourceID, filename: String): Unit = saveStringToFile(toJsonString(dsID), filename)
  def saveToDot(dsID: DataSourceID, filename: String): Unit = saveStringToFile(toDotString(dsID), filename)
}
