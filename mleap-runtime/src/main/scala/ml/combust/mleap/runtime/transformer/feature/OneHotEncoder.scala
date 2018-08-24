package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.OneHotEncoderModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.core.util.VectorConverters
import ml.combust.mleap.runtime.frame.{FrameBuilder, Row, SimpleTransformer, Transformer}
import ml.combust.mleap.runtime.function.{StructSelector, UserDefinedFunction}

import scala.util.Try

/**
  * Created by hollinwilkins on 5/10/16.
  */
case class OneHotEncoder(override val uid: String =
                           Transformer.uniqueName("one_hot_encoder"),
                         override val shape: NodeShape,
                         override val model: OneHotEncoderModel)
    extends SimpleTransformer {
//  override val exec: UserDefinedFunction = (value: Double) => model(Array(value)).head: Tensor[Double]
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
