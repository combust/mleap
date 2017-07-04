package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.CoalesceModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.Row
import ml.combust.mleap.runtime.function.{SchemaSpec, UserDefinedFunction}
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder

import scala.util.Try

/**
  * Created by hollinwilkins on 1/5/17.
  */
case class Coalesce(override val uid: String = Transformer.uniqueName("coalesce"),
                    override val shape: NodeShape,
                    model: CoalesceModel) extends Transformer {
  private val f = (values: Row) => model(values.toSeq: _*)
  val exec: UserDefinedFunction = UserDefinedFunction(f,
    ScalarType.Double,
    Seq(SchemaSpec(inputSchema)))

  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    builder.withOutput(shape.outputSchema.fields.head.name, shape.inputSchema.fields.map(_.name))(exec)
  }
}
