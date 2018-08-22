package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.OneHotEncoderModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.frame.{MultiTransformer, Row, Transformer}
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.core.util.VectorConverters

case class OneHotEncoderV23(override val uid: String =
                              Transformer.uniqueName("one_hot_encoder_v23"),
                            override val shape: NodeShape,
                            override val model: OneHotEncoderModel)
    extends MultiTransformer {
  override val exec: UserDefinedFunction = {
    // UGLY: I don't like this.  It's the best way forward from my current vantage point though.
    // This stems from the fact that anonymous functions in scala cannot have varargs parameters.
    shape.inputs.keys.size match {
      case 1 ⇒
        UserDefinedFunction(
          (v1: Double) =>
            Row(
              model(Array(v1))
                .map(VectorConverters.sparkVectorToMleapTensor): _*),
          outputSchema,
          inputSchema)
      case 2 ⇒
        UserDefinedFunction(
          (v1: Double, v2: Double) =>
            Row(
              model(Array(v1, v2))
                .map(VectorConverters.sparkVectorToMleapTensor): _*),
          outputSchema,
          inputSchema)
      case 3 ⇒
        UserDefinedFunction(
          (v1: Double, v2: Double, v3: Double) =>
            Row(
              model(Array(v1, v2, v3))
                .map(VectorConverters.sparkVectorToMleapTensor): _*),
          outputSchema,
          inputSchema)
      case 4 ⇒
        UserDefinedFunction(
          (v1: Double, v2: Double, v3: Double, v4: Double) =>
            Row(
              model(Array(v1, v2, v3, v4))
                .map(VectorConverters.sparkVectorToMleapTensor): _*),
          outputSchema,
          inputSchema)
      case 5 ⇒
        UserDefinedFunction(
          (v1: Double, v2: Double, v3: Double, v4: Double, v5: Double) =>
            Row(
              model(Array(v1, v2, v3, v4, v5))
                .map(VectorConverters.sparkVectorToMleapTensor): _*),
          outputSchema,
          inputSchema
        )
      case _ ⇒
        throw new IllegalArgumentException(
          "invalid number of inputs. unfortunately the current implementation of OneHotEncoder in mleap only supports 1-5 inputs.")
    }
  }
}
