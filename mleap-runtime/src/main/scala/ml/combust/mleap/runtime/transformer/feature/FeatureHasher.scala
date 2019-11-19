package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.FeatureHasherModel
import ml.combust.mleap.core.types.{NodeShape, SchemaSpec}
import ml.combust.mleap.runtime.frame.{FrameBuilder, Row, Transformer}
import ml.combust.mleap.runtime.function.{StructSelector, UserDefinedFunction}
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.core.util.VectorConverters._

import scala.util.Try

case class FeatureHasher(override val uid: String = Transformer.uniqueName("feature_hasher"),
                         override val shape: NodeShape,
                         override val model: FeatureHasherModel) extends Transformer {
  val exec: UserDefinedFunction = UserDefinedFunction(
    (values: Row) => model(values.toSeq): Tensor[Double],
    outputSchema.fields.head.dataType,
    Seq(SchemaSpec(inputSchema)))

  val outputCol: String = outputSchema.fields.head.name
  val inputCols: Seq[String] = inputSchema.fields.map(_.name)
  private val inputSelector: StructSelector = StructSelector(inputCols)

  override def transform[TB <: FrameBuilder[TB]](builder: TB): Try[TB] = {
    builder.withColumn(outputCol, inputSelector)(exec)
  }

}
