package ml.combust.mleap.xgboost.runtime

import biz.k11i.xgboost.Predictor
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Transformer}
import ml.combust.mleap.tensor.SparseTensor
import ml.combust.mleap.xgboost.runtime.testing.{BoosterUtils, BundleSerializationUtils, CachedDatasetUtils, ClassifierUtils, FloatingPointApproximations, RegressionUtils}
import ml.dmlc.xgboost4j.scala.Booster
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec
import XgbConverters._


class XGBoostPredictorRegressionModelParitySpec extends FunSpec
  with BoosterUtils
  with CachedDatasetUtils
  with ClassifierUtils
  with BundleSerializationUtils
  with FloatingPointApproximations
  with RegressionUtils {


  /**
   * A Predictor only provides a probability column for performance reasons
   */
  def probabilityColumnEqualityTest(mleapFrame1: DefaultLeapFrame, mleapFrame2: DefaultLeapFrame) = {
    val probabilityFrame1 = mleapFrame1.select("prediction").get
    val probabilityFrame2 = mleapFrame2.select("prediction").get

    val probabilityIndex = probabilityFrame1.schema.indexOf("prediction").get

    probabilityFrame1.dataset zip probabilityFrame2.dataset foreach {
      case (row1, row2) => {
        assert(
          almostEqual(
            row1.getDouble(probabilityIndex),
            row2.getDouble(probabilityIndex)
          )
        )
      }
    }
  }

  it("We can deserialize an xgboost object into a Predictor by changing the MLeapOp") {
    val xgboost4jTransformer = trainRegressor

    val mleapBundle = serializeModelToMleapBundle(xgboost4jTransformer)
    val deserializedPredictor: XGBoostPredictorRegression = loadXGBoostPredictorFromBundle(mleapBundle)
      .asInstanceOf[XGBoostPredictorRegression]

    assert(deserializedPredictor.model.predictor.isInstanceOf[Predictor])
  }

  it("A pre-serialization XGBoost4j model has the same results of a deserialized Predictor model") {
    val xgboost4jTransformer = trainRegressor

    val mleapBundle = serializeModelToMleapBundle(xgboost4jTransformer)
    val deserializedPredictor: Transformer = loadXGBoostPredictorFromBundle(mleapBundle)

    val preSerializationXGBoost4jResult = xgboost4jTransformer.transform(leapFrameBinomial).get
    val predictorModelResult = deserializedPredictor.transform(leapFrameBinomial).get

    probabilityColumnEqualityTest(preSerializationXGBoost4jResult, predictorModelResult)
  }

  it("A deserialized XGBoost4j has the same results of a deserialized Predictor"){
    val xgboost4jTransformer = trainRegressor

    val mleapBundle = serializeModelToMleapBundle(xgboost4jTransformer)

    val deserializedXGBoost4jTransformer: Transformer = loadMleapTransformerFromBundle(mleapBundle)
    val deserializedXGBoost4jResult = deserializedXGBoost4jTransformer.transform(leapFrameBinomial).get

    val deserializedPredictorTransformer: Transformer = loadXGBoostPredictorFromBundle(mleapBundle)
    val deserializedPredictorResult = deserializedPredictorTransformer.transform(leapFrameBinomial).get

    probabilityColumnEqualityTest(deserializedPredictorResult, deserializedXGBoost4jResult)
  }

  it("Predictor has the correct inputs and an output probability column") {
    val transformer = trainRegressor
    val mleapBundle = serializeModelToMleapBundle(transformer)

    val deserializedPredictorTransformer: Transformer = loadXGBoostPredictorFromBundle(mleapBundle)
    val numFeatures = deserializedPredictorTransformer.asInstanceOf[XGBoostPredictorRegression].model.numFeatures

    assert(
      deserializedPredictorTransformer.schema.fields ==
      Seq(
        StructField("features", TensorType(BasicType.Double, Seq(numFeatures))),
        StructField("prediction", ScalarType.Double.nonNullable)
      )
    )
  }

  it("XGBoost4j and Predictor results are the same when using a dense dataset") {
    val xgboost4jTransformer = trainRegressor

    val mleapBundle = serializeModelToMleapBundle(xgboost4jTransformer)
    val predictorTransformer: Transformer = loadXGBoostPredictorFromBundle(mleapBundle)

    val denseLeapFrame = toDenseFeaturesLeapFrame(leapFrameBinomial)

    val xgboost4jResult = xgboost4jTransformer.transform(denseLeapFrame).get
    val predictorResult = predictorTransformer.transform(denseLeapFrame).get

    probabilityColumnEqualityTest(xgboost4jResult, predictorResult)
  }
}
