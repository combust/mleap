package ml.combust.mleap.tensorflow

import ml.combust.mleap.core.types.{NodeShape, SchemaSpec}
import ml.combust.mleap.runtime.frame.{FrameBuilder, Row, Transformer}
import ml.combust.mleap.runtime.function.{StructSelector, UserDefinedFunction}
import ml.combust.mleap.tensor.Tensor

import scala.util.Try

/**
  * Created by hollinwilkins on 1/12/17.
  */
case class TensorflowTransformer(override val uid: String = Transformer.uniqueName("tensorflow"),
                                 override val shape: NodeShape,
                                 override val model: TensorflowModel) extends Transformer {

  val exec: UserDefinedFunction = {
    val f = (tensors: Row) => {
      Row(model(tensors.toSeq.map(_.asInstanceOf[Tensor[_]]): _*): _*)
    }

    UserDefinedFunction(f,
      outputSchema,
      Seq(SchemaSpec(inputSchema)))
  }

  val outputCols: Seq[String] = outputSchema.fields.map(_.name)
  val inputCols: Seq[String] = inputSchema.fields.map(_.name)
  private val inputSelector: StructSelector = StructSelector(inputCols)

  override def transform[TB <: FrameBuilder[TB]](builder: TB): Try[TB] = {
    builder.withColumns(outputCols, inputSelector)(exec)
  }

  override def close(): Unit = { model.close() }
}
