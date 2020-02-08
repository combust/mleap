package ml.combust.mleap.xgboost.runtime

import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Transformer}
import ml.combust.mleap.tensor.SparseTensor
import ml.combust.mleap.xgboost.runtime.testing.{BoosterUtils, BundleSerializationUtils, CachedDatasetUtils, FloatingPointApproximations}
import ml.dmlc.xgboost4j.scala.Booster
import org.scalatest.FunSpec


class XGBoostRegressionModelParitySpec extends FunSpec
  with BoosterUtils
  with CachedDatasetUtils
  with BundleSerializationUtils
  with FloatingPointApproximations {

  def createBoosterRegressor(booster: Booster): Transformer ={

    XGBoostRegression(
      "xgboostSingleThread",
      NodeShape.regression(),
      XGBoostRegressionModel(booster, numFeatures, 0)
    )
  }

  def equalityTestRowByRow(booster: Booster, mleapTransformer: Transformer) = {

    import XgbConverters._

    leapFrameLibSVMtest.dataset.foreach {
      r=>
        val mleapResult = mleapTransformer.transform(DefaultLeapFrame(mleapSchema.get, Seq(r))).get
        val mleapPredictionColIndex = mleapResult.schema.indexOf("prediction").get

        val singleRowDMatrix = r(1).asInstanceOf[SparseTensor[Double]].asXGB
        val boosterResult = booster.predict(singleRowDMatrix, false, 0).head(0)

        assert (boosterResult == mleapResult.dataset.head.getDouble(mleapPredictionColIndex))

    }
  }

  it("Results between the XGBoost4j booster and the MLeap Transformer are the same") {
    val booster = trainBooster(xgboostParams, denseDataset)
    val xgboostTransformer = createBoosterRegressor(trainBooster(xgboostParams, denseDataset))

    val mleapBundle = serializeModelToMleapBundle(xgboostTransformer)
    val deserializedTransformer: Transformer = loadMleapTransformerFromBundle(mleapBundle)

    equalityTestRowByRow(booster, deserializedTransformer)
  }

  it("has the correct inputs and outputs with columns: prediction, probability and raw_prediction") {

    val booster = trainBooster(xgboostParams, denseDataset)
    val transformer = createBoosterRegressor(booster)

    assert(transformer.schema.fields ==
      Seq(StructField("features", TensorType(BasicType.Double, Seq(numFeatures))),
        StructField("prediction", ScalarType.Double.nonNullable)))
  }

  it("Results are the same pre and post serialization") {
    val booster = trainBooster(xgboostParams, denseDataset)
    val xgboostTransformer = createBoosterRegressor(booster)

    val preSerializationResult = xgboostTransformer.transform(leapFrameLibSVMtrain)

    val mleapBundle = serializeModelToMleapBundle(xgboostTransformer)

    val deserializedTransformer: Transformer = loadMleapTransformerFromBundle(mleapBundle)
    val deserializedModelResult = deserializedTransformer.transform(leapFrameLibSVMtrain).get

    assert(preSerializationResult.get.dataset == deserializedModelResult.dataset)
  }
}
