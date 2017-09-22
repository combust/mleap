package ml.combust.mleap.tensorflow

import ml.combust.mleap.core.types.{NodeShape, SchemaSpec}
import ml.combust.mleap.runtime.Row
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{MultiTransformer, Transformer}
import ml.combust.mleap.tensor.Tensor

/**
  * Created by hollinwilkins on 1/12/17.
  */
case class TensorflowTransformer(override val uid: String = Transformer.uniqueName("tensorflow"),
                                 override val shape: NodeShape,
                                 override val model: TensorflowModel) extends MultiTransformer {
  private val f = (tensors: Row) => {
    model(tensors.toSeq.map(_.asInstanceOf[Tensor[_]]): _*)
  }
  override val exec: UserDefinedFunction = UserDefinedFunction(f,
    outputSchema,
    Seq(SchemaSpec(inputSchema)))

  override def close(): Unit = { model.close() }
}
