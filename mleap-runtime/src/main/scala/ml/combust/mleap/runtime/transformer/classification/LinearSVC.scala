package ml.combust.mleap.runtime.transformer.classification

import ml.combust.mleap.core.classification.LinearSVCModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.core.util.VectorConverters._
import ml.combust.mleap.runtime.frame.{MultiTransformer, Row, Transformer}

case class LinearSVC(
                    override val uid: String = Transformer.uniqueName("linear_svc"),
                    override val shape: NodeShape,
                    override val model: LinearSVCModel
                ) extends MultiTransformer
{
    override val exec: UserDefinedFunction =
    {
        val f = (shape.getOutput("raw_prediction")) match
        {
            case (Some(_)) =>
                (features: Tensor[Double]) =>
                {
                    val rawPrediction = model.predictRaw(features)
                    val prediction = model.rawToPrediction(rawPrediction)
                    Row(rawPrediction: Tensor[Double], prediction)
                }
            case (None) =>
                (features: Tensor[Double]) => Row(model(features))
        }

        UserDefinedFunction(f, outputSchema, inputSchema)
    }
}
