package ml.combust.mleap.tensorflow

import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder

import scala.util.Try

/**
  * Created by hollinwilkins on 1/12/17.
  */
case class TensorflowTransformer(override val uid: String = Transformer.uniqueName("tensorflow"),
                                 inputCols: Seq[String],
                                 outputCols: Seq[String],
                                 rawOutputCol: Option[String] = None,
                                 model: TensorflowModel) extends Transformer {
  val actualRawCol = rawOutputCol.getOrElse(uid)
  val exec: UserDefinedFunction = (tensors: Seq[Any]) => model()

  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    builder.withOutput(actualRawCol, inputCols.toArray)(exec)
  }
}
