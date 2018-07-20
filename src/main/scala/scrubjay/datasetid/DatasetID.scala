package scrubjay.datasetid

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonSubTypes, JsonTypeInfo}
import com.fasterxml.jackson.core.{JsonGenerator, JsonParser}
import com.fasterxml.jackson.databind._
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.parser.LegacyTypeStringParser
import org.apache.spark.sql.types.DataType
import scrubjay.datasetid.combination._
import scrubjay.datasetid.original._
import scrubjay.datasetid.transformation._
import scrubjay.dataspace.DimensionSpace
import scrubjay.schema.{ScrubJaySchema, SparkSchema}
import scrubjay.util.{readFileToString, writeStringToFile}

@JsonIgnoreProperties(
  value = Array("valid") // not sure why this gets populated
)
@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  include = JsonTypeInfo.As.PROPERTY,
  property = "type"
)
@JsonSubTypes(Array(
  new Type(value = classOf[OriginalDatasetID], name = "OriginalDatasetID"),
  new Type(value = classOf[Transformation], name = "Transformation"),
  new Type(value = classOf[Combination], name = "Combination")
))
abstract class DatasetID extends Serializable {

  def asOption(dimensionSpace: DimensionSpace): Option[DatasetID] = {
    if (isValid(dimensionSpace))
      Some(this)
    else
      None
  }
  def isValid(dimensionSpace: DimensionSpace = DimensionSpace.unknown): Boolean
  def scrubJaySchema(dimensionSpace: DimensionSpace = DimensionSpace.unknown): ScrubJaySchema
  def realize(dimensionSpace: DimensionSpace = DimensionSpace.unknown): DataFrame

  def debugPrint(dimensionSpace: DimensionSpace): Unit = {
    val df = realize(dimensionSpace)
    println(Console.YELLOW + "Spark Schema:")
    df.printSchema()
    println(Console.BLUE + "ScrubJay Schema:")
    println(scrubJaySchema(dimensionSpace))
    println(Console.GREEN + "Derivation Graph:")
    println(DatasetID.toAsciiGraphString(this))
    println(Console.RESET + "DataFrame:")
    df.show(false)
  }

  def dependencies: Seq[DatasetID]
}

object DatasetID {

  private[scrubjay] val objectMapper: ObjectMapper with ScalaObjectMapper = {
    val structTypeModule: SimpleModule = new SimpleModule()
    structTypeModule.addSerializer(classOf[SparkSchema], new SchemaSerializer())
    structTypeModule.addDeserializer(classOf[SparkSchema], new SchemaDeserializer())

    val m = new ObjectMapper with ScalaObjectMapper
    m.registerModule(DefaultScalaModule)
    m.registerModule(structTypeModule)
    m
  }

  def toHash(dsID: DatasetID): String = {
    import com.roundeights.hasher.Implicits._
    "h" + toJsonString(dsID).sha256.hex
  }

  def toDotGraphString(dsID: DatasetID): String = {

    val (nodes, edges) = toNodeEdgeTuple(dsID)

    val header = "digraph {"
    val nodeSection = nodes.map{case GraphNode(hash, derivation, columns) => {
      val columnString = columns.mkString(" X\n")
      val style = derivation match {
        case "NaturalJoin" => "style=filled, fillcolor=\"forestgreen\", label=\"NaturalJoin\\n" + columnString + "\""
        case "InterpolationJoin" => "style=filled, fillcolor=\"lime\", label=\"InterpolationJoin\\n" + columnString + "\""
        case "ExplodeDiscreteRange" => "style=filled, fillcolor=\"deepskyblue\", label=\"ExplodeDiscrete\\n" + columnString + "\""
        case  "ExplodeContinuousRange" =>"style=filled, fillcolor=\"lightskyblue\", label=\"ExplodeContinuous\\n" + columnString + "\""
        case "CSVDataset" =>"style=filled, fillcolor=\"darkorange\", label=\"CSV\\n" + columnString + "\""
        case "CassandraDataset" => "style=filled, fillcolor=\"darkorange\", label=\"Cassandra\\n" + columnString + "\""
        case "LocalDataset" => "style=filled, fillcolor=\"darkorange\", label=\"Local\\n" + columnString + "\""
        case "UNKNOWN" => "label=\"UNKNOWN\""
      }
      "\t" + hash + " [" + style + "];"
    }}.mkString("\n")
    val edgeSection = edges.map{case GraphEdge(left, right) => {
      "\t\t" + left.hash + " -> " + right.hash + " [penwidth=2];"
    }}.mkString("\n")
    val footer = "}"

    Seq(header, nodeSection, edgeSection, footer).mkString("\n")
  }

  def toAsciiGraphString(dsID: DatasetID): String = {
    import com.github.mdr.ascii.graph.Graph
    import com.github.mdr.ascii.layout.GraphLayout

    def node2AsciiVertex(n: GraphNode): String = n.derivation + "\n" + n.columns.mkString(",\n")

    val (nodes, edges) = toNodeEdgeTuple(dsID)
    val graph = Graph(
      nodes.map(node2AsciiVertex).toSet,
      edges.map(e => node2AsciiVertex(e.left) -> node2AsciiVertex(e.right)).toList
    )

    GraphLayout.renderGraph(graph)
  }

  def fromJsonFile(filename: String): DatasetID = {
    fromJsonString(readFileToString(filename))
  }

  def fromJsonString(json: String): DatasetID = {
    objectMapper.readValue[DatasetID](json, classOf[DatasetID])
  }

  def toJsonString(dsID: DatasetID): String = {
    objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(dsID)
  }

  def writeToJsonFile(dsID: DatasetID, filename: String): Unit = {
    writeStringToFile(toJsonString(dsID), filename)
  }

  def writeToDotFile(dsID: DatasetID, filename: String): Unit = {
    writeStringToFile(toDotGraphString(dsID), filename)
  }

  sealed case class GraphNode(hash: String, derivation: String, columns: Set[String])
  sealed case class GraphEdge(left: GraphNode, right: GraphNode)

  private def toNodeEdgeTuple(dsID: DatasetID, parent: Option[GraphNode] = None): (Seq[GraphNode], Seq[GraphEdge]) = {

    val hash: String = toHash(dsID)

    val derivation = dsID match {
      case _: NaturalJoin => "NaturalJoin"
      case _: InterpolationJoin => "InterpolationJoin"
      case _: ExplodeList => "ExplodeDiscreteRange"
      case _: ExplodeRange =>  "ExplodeContinuousRange"
      case _: CSVDatasetID => "CSVDataset"
      case _: CassandraDatasetID => "CassandraDataset"
      case _: LocalDatasetID => "LocalDataset"
      case _ => "UNKNOWN"
    }

    val columns = dsID.scrubJaySchema(DimensionSpace.unknown).fieldNames

    val node = GraphNode(hash, derivation, columns)

    val edge = {
      if (parent.isDefined)
        Seq(GraphEdge(node, parent.get))
      else
        Seq()
    }

    val (childNodes, childEdges) = dsID.dependencies
      .map(toNodeEdgeTuple(_, Some(node)))
      .fold((Seq.empty, Seq.empty))((a, b) => (a._1 ++ b._1, a._2 ++ b._2))

    (node +: childNodes, edge ++ childEdges)
  }

  /**
    * Serializer/Deserializer for SparkSchema (Spark DataFrame StructType)
    */

  class SchemaSerializer extends JsonSerializer[SparkSchema] {
    override def serialize(value: SparkSchema, gen: JsonGenerator, serializers: SerializerProvider): Unit = {
      import org.apache.spark.sql.types.scrubjayunits._
      import org.json4s.JsonAST.JValue
      import org.json4s.jackson.JsonMethods

      val jValue: JValue = value.getJValue
      val jNode = JsonMethods.asJsonNode(jValue)
      gen.writeTree(jNode)
    }
  }

  class SchemaDeserializer extends JsonDeserializer[SparkSchema] {
    override def deserialize(p: JsonParser, ctxt: DeserializationContext): SparkSchema = {
      val json = p.readValueAsTree().toString
      scala.util.Try(DataType.fromJson(json)).getOrElse(LegacyTypeStringParser.parse(json)) match {
        case t: SparkSchema => t
        case _ => throw new RuntimeException(s"Failed parsing SparkSchema: $json")
      }
    }
  }
}
