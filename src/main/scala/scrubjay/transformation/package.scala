package scrubjay

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Expression

package object transformation {

  def withExpr(expr: Expression): Column = new Column(expr)
}
