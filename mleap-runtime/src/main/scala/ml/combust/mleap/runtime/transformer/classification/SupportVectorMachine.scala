package ml.combust.mleap.runtime.transformer.classification

import ml.combust.mleap.core.classification.SupportVectorMachineModel
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import org.apache.spark.ml.linalg.Vector

import scala.util.Try

/**
  * Created by hollinwilkins on 4/14/16.
  */
case class SupportVectorMachine(override val uid: String = Transformer.uniqueName("support_vector_machine"),
                                featuresCol: String,
                                predictionCol: String,
                                probabilityCol: Option[String] = None,
                                model: SupportVectorMachineModel) extends Transformer {
  val predictProbability: UserDefinedFunction = (features: Vector) => model.predictBinaryProbability(features)
  val probabilityToPrediction: UserDefinedFunction = (probability: Double) => model.binaryProbabilityToPrediction(probability)
  val exec: UserDefinedFunction = (features: Vector) => model(features)

  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    probabilityCol match {
      case Some(p) =>
        for(b <- builder.withOutput(p, featuresCol)(predictProbability);
            b2 <- b.withOutput(predictionCol, p)(probabilityToPrediction)) yield b2
      case None =>
        builder.withOutput(predictionCol, featuresCol)(exec)
    }
  }
}
