package ml.combust.mleap.xgboost.runtime

import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Transformer}
import ml.combust.mleap.tensor.Tensor
import ml.combust.mleap.xgboost.runtime.testing.{BoosterUtils, BundleSerializationUtils, CachedDatasetUtils, FloatingPointApproximations, RegressionUtils}
import ml.dmlc.xgboost4j.scala.Booster
import org.scalatest.FunSpec
import XgbConverters._


class XGBoostRegressionModelParitySpec extends FunSpec
  with BoosterUtils
  with CachedDatasetUtils
  with BundleSerializationUtils
  with FloatingPointApproximations {



  def equalityTestRowByRow(booster: Booster, mleapTransformer: Transformer, leapFrameDataset: DefaultLeapFrame) = {

    val featuresColumnIndex = leapFrameDataset.schema.indexOf("features").get

    leapFrameDataset.dataset.foreach {
      r=>
        val mleapResult = mleapTransformer.transform(DefaultLeapFrame(leapFrameDataset.schema, Seq(r))).get
        val mleapPredictionColIndex = mleapResult.schema.indexOf("prediction").get

        val singleRowDMatrix = r(featuresColumnIndex).asInstanceOf[Tensor[Double]].asXGB
        val boosterResult = booster.predict(singleRowDMatrix, false, 0).head(0)

        assert (boosterResult == mleapResult.dataset.head.getDouble(mleapPredictionColIndex))

    }
  }

  it("Results between the XGBoost4j booster and the MLeap Transformer are the same") {
    equalityTestRowByRow(RegressionUtils.xgboostBooster, RegressionUtils.deserializedMleapTransformer, leapFrameBinomial)
  }

  it("has the correct inputs and outputs with columns: prediction, probability and raw_prediction") {
    val numFeatures = RegressionUtils.mleapTransformer.asInstanceOf[XGBoostRegression].model.numFeatures
    assert(RegressionUtils.mleapTransformer.schema.fields ==
      Seq(StructField("features", TensorType(BasicType.Double, Seq(numFeatures))),
        StructField("prediction", ScalarType.Double.nonNullable)))
  }

  it("Results are the same pre and post serialization") {
    val preSerializationResult = RegressionUtils.mleapTransformer.transform(leapFrameBinomial).get
    val deserializedModelResult = RegressionUtils.deserializedMleapTransformer.transform(leapFrameBinomial).get
    assert(preSerializationResult.dataset == deserializedModelResult.dataset)
  }

  it("Test results are the same when using a dense dataset") {
    val denseLeapFrame = toDenseFeaturesLeapFrame(leapFrameBinomial)
    val preSerializationResult = RegressionUtils.mleapTransformer.transform(denseLeapFrame).get
    val deserializedResult = RegressionUtils.deserializedMleapTransformer.transform(denseLeapFrame).get
    assert(preSerializationResult.dataset == deserializedResult.dataset)
  }
}
