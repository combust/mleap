package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.VectorAssemblerModel
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder

import scala.util.Try

/**
  * Created by hwilkins on 10/23/15.
  */
case class VectorAssembler(override val uid: String = Transformer.uniqueName("vector_assembler"),
                           inputCols: Array[String],
                           outputCol: String) extends Transformer {
  val exec = (values: Array[Any]) => VectorAssemblerModel.default(values)

  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    builder.withOutput(outputCol, inputCols)(exec)
  }
}
