package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.Transformer
import ml.combust.mleap.core.feature.CoalesceModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.core.frame.Row
import ml.combust.mleap.core.function.UserDefinedFunction
import ml.combust.mleap.core.frame.TransformBuilder

import scala.util.Try

/**
  * Created by hollinwilkins on 1/5/17.
  */
case class Coalesce(override val uid: String = Transformer.uniqueName("coalesce"),
                    override val shape: NodeShape,
                    override val model: CoalesceModel) extends Transformer {
  val inputs: Seq[String] = shape.inputs.values.map(_.name).toSeq
  val outputCol: String = shape.standardOutput.name

  private val f = (values: Row) => model(values.toSeq: _*)
  val exec: UserDefinedFunction = UserDefinedFunction(f,
    ScalarType.Double,
    Seq(SchemaSpec(inputSchema)))

  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    builder.withColumn(outputCol, inputs)(exec)
  }
}
