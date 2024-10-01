package ml.combust.mleap.tensorflow

import ml.combust.mleap.core.types.NodeShape
import ml.combust.mleap.runtime.frame.{FrameBuilder, Row, SimpleTransformer, Transformer}
import ml.combust.mleap.runtime.function.{FieldSelector, Selector, UserDefinedFunction}
import ml.combust.mleap.tensor.Tensor

import scala.util.Try

case class BatchTensorflowTransformer(override val uid: String = Transformer.uniqueName("batch_tensorflow"),
                                      override val shape: NodeShape,
                                      override val model: BatchTensorflowModel)
  extends SimpleTransformer {
  private val f = (rows: Seq[Row]) => {
    model(rows.map(x => x.toSeq.map(Tensor.scalar(_))): _*).map(Row(_: _*))
  }

  override val exec: UserDefinedFunction =
    UserDefinedFunction(f, outputSchema, inputSchema)

  val outputCols: Seq[String] = outputSchema.fields.map(_.name)
  val inputCols: Seq[String] = inputSchema.fields.map(_.name)
  private val inputSelector: Seq[Selector] = inputCols.map(FieldSelector)

  override def transform[TB <: FrameBuilder[TB]](builder: TB): Try[TB] = {
    builder.withColumns(outputCols, inputSelector: _*)(exec)
  }

  override def close(): Unit = { model.close() }
}