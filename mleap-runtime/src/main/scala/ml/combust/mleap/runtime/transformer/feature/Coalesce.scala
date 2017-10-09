package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.CoalesceModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.frame.{FrameBuilder, Row, Transformer}
import ml.combust.mleap.runtime.function.UserDefinedFunction

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

  override def transform[TB <: FrameBuilder[TB]](builder: TB): Try[TB] = {
    builder.withColumn(outputCol, inputs)(exec)
  }
}
