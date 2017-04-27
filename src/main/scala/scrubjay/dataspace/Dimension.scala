package scrubjay.dataspace

case class Dimension(name: String, ordered: Boolean, continuous: Boolean)

case class DimensionSpace(dimensions: Array[Dimension])

object DimensionSpace {
  def empty: DimensionSpace = DimensionSpace(Array())
}

