package ml.combust.mleap.runtime.transformer.feature

import ml.combust.mleap.core.feature.ReverseStringIndexerModel
import ml.combust.mleap.runtime.function.UserDefinedFunction
import ml.combust.mleap.runtime.transformer.{FeatureTransformer, Transformer}
import ml.combust.mleap.runtime.types.{DoubleType, StringType, StructField, TensorType}

import scala.util.{Success, Try}

/**
  * Created by hollinwilkins on 3/30/16.
  */
case class ReverseStringIndexer(override val uid: String = Transformer.uniqueName("reverse_string_indexer"),
                                override val inputCol: String,
                                override val outputCol: String,
                                model: ReverseStringIndexerModel) extends FeatureTransformer {
  override val exec: UserDefinedFunction = (value: Double) => model(value.toInt)

  override def getFields(): Try[Seq[StructField]] = Success(Seq(
    StructField(inputCol, DoubleType()),
    StructField(outputCol, StringType())))
}
