package ml.combust.mleap.core.feature

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.types.{ScalarType, StructType}

import scala.util.matching.Regex

case class RegexIndexerModel(lookup: Seq[(Regex, Int)],
                             defaultIndex: Option[Int]) extends Model {
  def apply(value: String): Int = {
    lookup.find {
      case (r, _) => r.findFirstMatchIn(value).isDefined
    }.map(_._2).orElse(defaultIndex).get
  }

  override def inputSchema: StructType = StructType("input" -> ScalarType.String).get

  override def outputSchema: StructType = StructType("output" -> ScalarType.Int.nonNullable).get
}
