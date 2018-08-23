package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.OneHotEncoderModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.frame._
import ml.combust.mleap.runtime.function.{StructSelector, UserDefinedFunction}
import ml.combust.mleap.core.util.VectorConverters

import scala.util.Try

case class OneHotEncoderV23(override val uid: String =
                              Transformer.uniqueName("one_hot_encoder_v23"),
                            override val shape: NodeShape,
                            override val model: OneHotEncoderModel)
    extends MultiTransformer {

  // Lot of things going on here:
  // o Convert values into an array
  // o Invoke 1HE model
  // o Convert spark tensor results to mleap tensors
  // o Spread results and use as input to new Row
  private val f = (values: Row) => {
    val v = values.toSeq.asInstanceOf[Seq[Double]].toArray
    val res = model(v).map(VectorConverters.sparkVectorToMleapTensor)
    Row(res: _*)
  }
  val exec: UserDefinedFunction =
    UserDefinedFunction(f, outputSchema, Seq(SchemaSpec(inputSchema)))

  val outputCols: Seq[String] = outputSchema.fields.map(_.name)
  val inputCols: Seq[String] = inputSchema.fields.map(_.name)
  private val inputSelector: StructSelector = StructSelector(inputCols)

  override def transform[TB <: FrameBuilder[TB]](builder: TB): Try[TB] = {
    builder.withColumns(outputCols, inputSelector)(exec)
  }
}
