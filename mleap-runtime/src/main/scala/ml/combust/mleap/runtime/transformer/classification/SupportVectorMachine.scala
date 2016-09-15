package ml.combust.mleap.runtime.transformer.classification

import ml.combust.mleap.core.classification.SupportVectorMachineModel
import ml.combust.mleap.runtime.transformer.Transformer
import ml.combust.mleap.runtime.transformer.builder.TransformBuilder
import ml.combust.mleap.runtime.types.{DoubleType, TensorType}

import scala.util.Try

/**
  * Created by hollinwilkins on 4/14/16.
  */
case class SupportVectorMachine(uid: String = Transformer.uniqueName("support_vector_machine"),
                                featuresCol: String,
                                predictionCol: String,
                                model: SupportVectorMachineModel) extends Transformer {
  override def transform[TB <: TransformBuilder[TB]](builder: TB): Try[TB] = {
    builder.withInput(featuresCol, TensorType.doubleVector()).flatMap {
      case(b, featuresIndex) =>
        b.withOutput(predictionCol, DoubleType)(row => model(row.getVector(featuresIndex)))
    }
  }
}
