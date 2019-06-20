package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.VectorAssemblerModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.core.util.VectorConverters._
import ml.combust.mleap.runtime.frame.{FrameBuilder, Row, Transformer}
import ml.combust.mleap.runtime.function.{StructSelector, UserDefinedFunction}

import scala.util.Try

/**
  * Created by hwilkins on 10/23/15.
  */
case class VectorAssembler(override val uid: String = Transformer.uniqueName("vector_assembler"),
                           override val shape: NodeShape,
                           override val model: VectorAssemblerModel) extends Transformer {
  val exec: UserDefinedFunction = UserDefinedFunction(
    (values: Row) => model(values.toSeq): Tensor[Double],
    outputSchema.fields.head.dataType,
    Seq(SchemaSpec(inputSchema)))

  val outputCol: String = outputSchema.fields.head.name
  val inputCols: Seq[String] = inputSchema.fields.map(_.name)
  private val inputSelector: StructSelector = StructSelector(inputCols)

  override def transform[TB <: FrameBuilder[TB]](builder: TB): Try[TB] = {
    builder.withColumn(outputCol, inputSelector)(exec)
  }
}
