package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.ImputerModel
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{FeatureTransformer, Transformer}

import scala.util.{Success, Try}

/**
  * Created by mikhail on 12/18/16.
  */
case class Imputer(override val uid: String = Transformer.uniqueName("imputer"),
                   override val inputCol: String,
                   override val outputCol: String,
                   model: ImputerModel) extends FeatureTransformer {
  override val exec: UserDefinedFunction = if(model.inputNullable) {
    (value: Option[Double]) => model(value)
  } else {
    (value: Double) => model(value)
  }

  override def getFields(): Try[Seq[StructField]] = {
    Success(Seq(StructField(inputCol, ScalarType(BasicType.Double, model.inputNullable)),
      StructField(outputCol, ScalarType.Double)))
  }
}
