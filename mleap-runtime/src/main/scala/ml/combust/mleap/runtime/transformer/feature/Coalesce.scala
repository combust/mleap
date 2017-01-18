package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.CoalesceModel
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder

import scala.util.Try

/**
  * Created by hollinwilkins on 1/5/17.
  */
case class Coalesce(override val uid: String = Transformer.uniqueName("coalesce"),
                    inputCols: Array[String],
                    outputCol: String,
                    model: CoalesceModel) extends Transformer {
  val exec: UserDefinedFunction = (values: Seq[Any]) => model(values: _*)

  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    builder.withOutput(outputCol, inputCols)(exec)
  }
}
