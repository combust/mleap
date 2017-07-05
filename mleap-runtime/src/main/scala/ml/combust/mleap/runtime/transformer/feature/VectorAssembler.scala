package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.VectorAssemblerModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.core.util.VectorConverters._
import ml.combust.mleap.runtime.Row
import ml.combust.mleap.runtime.function.UserDefinedFunction

import scala.util.Try

/**
  * Created by hwilkins on 10/23/15.
  */
case class VectorAssembler(override val uid: String = Transformer.uniqueName("vector_assembler"),
                           override val shape: NodeShape,
                           model: VectorAssemblerModel) extends Transformer {
  private val f = (values: Row) => model(values.toSeq): Tensor[Double]
  val exec: UserDefinedFunction = UserDefinedFunction(f,
    outputSchema.fields.head.dataType,
    Seq(SchemaSpec(inputSchema)))

  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    builder.withOutput(outputSchema.fields.head.name, inputSchema.fields.map(_.name))(exec)
  }
}
