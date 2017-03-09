package scrubjay.datasource



import scrubjay.metasource._
import com.roundeights.hasher.Implicits._


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

  protected def toNodeEdgeTuple(dsID: DataSourceID, parentName: Option[String] = None): (String, String) = {
    import scrubjay.util._

    val hash = toHash(dsID)
    // node_type = d.get('$type')
    // node_color = 'white'
    //
    // if 'datasource' in node_type:
    //     node_color = 'orange'
    // elif 'Join' in node_type:
    //     node_color = 'lightgreen'
    // elif 'derivation' in node_type:
    //     node_color = 'lightblue'
    //
    // name = node_type.split('.')[-1]
    // node = [key + ' [label = "' + name + '", fontname="helvetica", penwidth=2, style=filled, fillcolor=' + node_color + ']']
    // edge = [p + ' -> ' + key + '[penwidth=2]'] if p != '' else []
    val edge = parentName.ifDefinedThen(p => Seq(p + " -> " + hash))(Some(Seq.empty))
    // children = map(lambda k: d[k], filter(lambda a: a != '$type', d.keys()))
    // children_nodes = []
    // children_edges = []
    // for child in children:
    //     (child_nodes, child_edges) = json_to_dot_data(child, key)
    //     children_nodes = children_nodes + child_nodes
    //     children_edges = children_edges + child_edges

    // return (node + children_nodes, edge + children_edges)
    ???
  }
  def toDotString(dsID: DataSourceID): String = {

    // digraph_data = json_to_dot_data(derivation)

    // digraph_source = 'digraph "Derivations" {\n\t' + '\n\t'.join(digraph_data[0]) + '\n\t\t' + '\n\t\t'.join(digraph_data[1]) + ' }'

    ???
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
