package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.InteractionModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.core.util.VectorConverters._
import ml.combust.mleap.runtime.frame.{BaseTransformer, FrameBuilder, Row, Transformer}

import scala.util.Try

/**
  * Created by hollinwilkins on 4/26/17.
  */
case class Interaction(override val uid: String = Transformer.uniqueName("interaction"),
                       override val shape: NodeShape,
                       override val model: InteractionModel) extends BaseTransformer {
  val exec: UserDefinedFunction = {
    UserDefinedFunction(
      (row: Row) => model(row.toSeq),
      TensorType(BasicType.Double, Seq(model.outputSize)),
      Seq(SchemaSpec(inputSchema)))
  }

  override def transform[TB <: FrameBuilder[TB]](builder: TB): Try[TB] = {
    builder.withColumn(shape.standardOutput.name, inputs)(exec)
  }
}
