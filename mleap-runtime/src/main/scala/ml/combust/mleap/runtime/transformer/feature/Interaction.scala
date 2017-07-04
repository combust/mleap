package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.InteractionModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.Row
import ml.combust.mleap.runtime.function.{SchemaSpec, UserDefinedFunction}
import ml.combust.mleap.runtime.transformer.{BaseTransformer, Transformer}
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.core.util.VectorConverters._

import scala.util.Try

/**
  * Created by hollinwilkins on 4/26/17.
  */
case class Interaction(override val uid: String = Transformer.uniqueName("interaction"),
                       override val shape: NodeShape,
                       model: InteractionModel) extends BaseTransformer {
  private val f = (row: Row) => model(row.toSeq): Tensor[Double]
  val exec: UserDefinedFunction = {
    UserDefinedFunction(f,
      TensorType(BasicType.Double),
      Seq(SchemaSpec(inputSchema)))
  }

  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    builder.withOutput(shape.outputs.head.field.name, inputs)(exec)
  }
}
