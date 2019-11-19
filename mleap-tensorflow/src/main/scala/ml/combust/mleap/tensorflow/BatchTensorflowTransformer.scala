package ml.combust.mleap.tensorflow

import ml.combust.mleap.core.types.{NodeShape, SchemaSpec}
import ml.combust.mleap.runtime.frame.{FrameBuilder, Row, Transformer}
import ml.combust.mleap.runtime.function.{StructSelector, UserDefinedFunction}
import ml.combust.mleap.tensor.Tensor

import scala.util.Try

case class BatchTensorflowTransformer(override val uid: String = Transformer.uniqueName("batchTensorflow"),
                                      override val shape: NodeShape,
                                      override val model: BatchTensorflowModel) extends Transformer {
  private val f = (tensors: Seq[Row]) => {
    model(tensors.map(_.toSeq.map(Tensor.scalar(_))):_*).transpose.map(x=>Row(x:_*))
  }

  val exec: UserDefinedFunction = UserDefinedFunction(f,
    outputSchema,
    Seq(SchemaSpec(inputSchema)))

  val outputCols: Seq[String] = outputSchema.fields.map(_.name)
  val inputCols: Seq[String] = inputSchema.fields.map(_.name)
  private val inputSelector: StructSelector = StructSelector(inputCols)

  override def transform[TB <: FrameBuilder[TB]](builder: TB): Try[TB] = {
    builder.withColumns(outputCols, inputSelector)(exec)
  }

  override def close(): Unit = { model.close() }
}