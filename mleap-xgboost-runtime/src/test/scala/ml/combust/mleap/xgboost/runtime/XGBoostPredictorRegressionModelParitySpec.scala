package ml.combust.mleap.xgboost.runtime

import biz.k11i.xgboost.Predictor
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Transformer}
import ml.combust.mleap.xgboost.runtime.testing.{BoosterUtils, BundleSerializationUtils, CachedDatasetUtils, ClassifierUtils, FloatingPointApproximations, RegressionUtils}
import org.scalatest.funspec.AnyFunSpec
import XgbConverters._


class XGBoostPredictorRegressionModelParitySpec extends org.scalatest.funspec.AnyFunSpec
  with BoosterUtils
  with CachedDatasetUtils
  with BundleSerializationUtils
  with FloatingPointApproximations {

  /**
   * A Predictor only provides a prediction column for performance reasons
   */
  def predictionColumnEqualityTest(mleapFrame1: DefaultLeapFrame, mleapFrame2: DefaultLeapFrame) = {
    val predictionFrame1 = mleapFrame1.select("prediction").get
    val predictionFrame2 = mleapFrame2.select("prediction").get

    val predictionIndex = predictionFrame1.schema.indexOf("prediction").get

    predictionFrame1.dataset zip predictionFrame2.dataset foreach {
      case (row1, row2) => {
        assert(
          almostEqual(
            row1.getDouble(predictionIndex),
            row2.getDouble(predictionIndex)
          )
        )
      }
    }
  }

  it("We can deserialize an xgboost object into a Predictor by changing the MLeapOp") {
    val deserializedPredictor: XGBoostPredictorRegression = RegressionUtils.deserializedXGBoostPredictor.asInstanceOf[XGBoostPredictorRegression]
    assert(deserializedPredictor.model.predictor.isInstanceOf[Predictor])
  }

  it("A pre-serialization XGBoost4j model has the same results of a deserialized Predictor model") {
    val preSerializationXGBoost4jResult = RegressionUtils.mleapTransformer.transform(leapFrameBinomial).get
    val predictorModelResult = RegressionUtils.deserializedXGBoostPredictor.transform(leapFrameBinomial).get
    predictionColumnEqualityTest(preSerializationXGBoost4jResult, predictorModelResult)
  }

  it("A deserialized XGBoost4j has the same results of a deserialized Predictor"){
    assert(RegressionUtils.deserializedMleapTransformer.isInstanceOf[XGBoostRegression])
    assert(RegressionUtils.deserializedXGBoostPredictor.isInstanceOf[XGBoostPredictorRegression])
    val deserializedXGBoost4jResult = RegressionUtils.deserializedMleapTransformer.transform(leapFrameBinomial).get
    val deserializedPredictorResult = RegressionUtils.deserializedXGBoostPredictor.transform(leapFrameBinomial).get
    predictionColumnEqualityTest(deserializedPredictorResult, deserializedXGBoost4jResult)
  }

  it("Predictor has the correct inputs and an output probability column") {
    val numFeatures = RegressionUtils.deserializedXGBoostPredictor.asInstanceOf[XGBoostPredictorRegression].model.numFeatures
    assert(
      RegressionUtils.deserializedXGBoostPredictor.schema.fields ==
      Seq(
        StructField("features", TensorType(BasicType.Double, Seq(numFeatures))),
        StructField("prediction", ScalarType.Double.nonNullable)
      )
    )
  }

  it("XGBoost4j and Predictor results are the same when using a dense dataset") {
    val denseLeapFrame = toDenseFeaturesLeapFrame(leapFrameBinomial)
    val xgboost4jResult = RegressionUtils.mleapTransformer.transform(denseLeapFrame).get
    val predictorResult = RegressionUtils.deserializedXGBoostPredictor.transform(denseLeapFrame).get
    predictionColumnEqualityTest(xgboost4jResult, predictorResult)
  }
}
