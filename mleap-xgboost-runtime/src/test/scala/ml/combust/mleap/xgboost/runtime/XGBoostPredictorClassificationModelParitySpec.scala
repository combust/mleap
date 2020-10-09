package ml.combust.mleap.xgboost.runtime

import biz.k11i.xgboost.Predictor
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Transformer}
import ml.combust.mleap.tensor.SparseTensor
import ml.combust.mleap.xgboost.runtime.testing.{BoosterUtils, BundleSerializationUtils, CachedDatasetUtils, ClassifierUtils, FloatingPointApproximations}
import ml.dmlc.xgboost4j.scala.Booster
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec
import XgbConverters._


class XGBoostPredictorClassificationModelParitySpec extends FunSpec
  with BoosterUtils
  with CachedDatasetUtils
  with ClassifierUtils
  with BundleSerializationUtils
  with FloatingPointApproximations {

  def equalityTestRowByRowMultinomialProbability(
                                       booster: Booster,
                                       mleapTransformer: Transformer,
                                       leapFrameDataset: DefaultLeapFrame) = {


    val featuresColumnIndex = leapFrameDataset.schema.indexOf("features").get

    leapFrameDataset.dataset.foreach {
      r =>
        val mleapResult = mleapTransformer.transform(DefaultLeapFrame(leapFrameDataset.schema, Seq(r))).get
        val mleapProbabilityColIndex = mleapResult.schema.indexOf("probability").get

        val singleRowDMatrix = r(featuresColumnIndex).asInstanceOf[SparseTensor[Double]].asXGB
        val boosterResult = booster.predict(singleRowDMatrix, false, 0).head
        val boosterProbability = Vectors.dense(boosterResult.map(_.toDouble)).toDense

        assert(
          almostEqualSequences(
            Seq(boosterProbability.values),
            Seq(mleapResult.dataset.head.getTensor[Double](mleapProbabilityColIndex).toArray)
          )
        )
    }
  }

  /**
    * A Predictor only provides a probability column for performance reasons
    */
  def probabilityColumnEqualityTest(mleapFrame1: DefaultLeapFrame, mleapFrame2: DefaultLeapFrame) = {
    val probabilityFrame1 = mleapFrame1.select("probability").get
    val probabilityFrame2 = mleapFrame2.select("probability").get

    val probabilityIndex = probabilityFrame1.schema.indexOf("probability").get

    probabilityFrame1.dataset zip probabilityFrame2.dataset foreach {
      case (row1, row2) => {
        assert(
          almostEqualSequences(
            Seq(row1.getTensor[Double](probabilityIndex).toArray),
            Seq(row2.getTensor[Double](probabilityIndex).toArray)))
      }
    }
  }

  it("We can deserialize an xgboost object into a Predictor by changing the MLeapOp") {
    val xgboost4jTransformer = trainXGBoost4jClassifier

    val mleapBundle = serializeModelToMleapBundle(xgboost4jTransformer)
    val deserializedPredictor: XGBoostPredictorClassification = loadXGBoostPredictorFromBundle(mleapBundle)
      .asInstanceOf[XGBoostPredictorClassification]

    assert(deserializedPredictor.model.impl.predictor.isInstanceOf[Predictor])
  }

  it("A pre-serialization XGBoost4j model has the same results of a deserialized Predictor model") {
    val xgboost4jTransformer = trainXGBoost4jClassifier

    val mleapBundle = serializeModelToMleapBundle(xgboost4jTransformer)
    val deserializedPredictor: Transformer = loadXGBoostPredictorFromBundle(mleapBundle)

    val preSerializationXGBoost4jResult = xgboost4jTransformer.transform(leapFrameBinomial).get
    val predictorModelResult = deserializedPredictor.transform(leapFrameBinomial).get

    probabilityColumnEqualityTest(preSerializationXGBoost4jResult, predictorModelResult)
  }

  it("A deserialized XGBoost4j has the same results of a deserialized Predictor"){
    val xgboost4jTransformer = trainXGBoost4jClassifier

    val mleapBundle = serializeModelToMleapBundle(xgboost4jTransformer)

    val deserializedXGBoost4jTransformer: Transformer = loadMleapTransformerFromBundle(mleapBundle)
    val deserializedXGBoost4jResult = deserializedXGBoost4jTransformer.transform(leapFrameBinomial).get

    val deserializedPredictorTransformer: Transformer = loadXGBoostPredictorFromBundle(mleapBundle)
    val deserializedPredictorResult = deserializedPredictorTransformer.transform(leapFrameBinomial).get

    probabilityColumnEqualityTest(deserializedPredictorResult, deserializedXGBoost4jResult)
  }

  it("Predictor has the correct inputs and an output probability column") {
    val transformer = trainXGBoost4jClassifier
    val mleapBundle = serializeModelToMleapBundle(transformer)

    val deserializedPredictorTransformer: Transformer = loadXGBoostPredictorFromBundle(mleapBundle)
    val numFeatures = deserializedPredictorTransformer.asInstanceOf[XGBoostPredictorClassification].model.numFeatures

    assert(deserializedPredictorTransformer.schema.fields ==
      Seq(StructField("features", TensorType(BasicType.Double, Seq(numFeatures))),
        StructField("probability", TensorType(BasicType.Double, Seq(2)))))
  }

  it("[Multinomial] An XGBoost4j Booster has the same results as a deserialized Predictor"){
    val multiBooster = trainMultinomialBooster(multinomialDataset)

    val mleapBundle = serializeModelToMleapBundle(trainMultinomialXGBoost4jClassifier)
    val deserializedPredictorTransformer: Transformer = loadXGBoostPredictorFromBundle(mleapBundle)

    equalityTestRowByRowMultinomialProbability(multiBooster, deserializedPredictorTransformer, leapFrameMultinomial)
  }

  it("[Multinomial] XGBoostPredictorMultinomialClassificationModel results are the same pre and post serialization") {
    val xgboost4jTransformer = trainMultinomialXGBoost4jClassifier

    val mleapBundle = serializeModelToMleapBundle(xgboost4jTransformer)
    val predictorTransformer: Transformer = loadXGBoostPredictorFromBundle(mleapBundle)

    val xgboost4jResult = xgboost4jTransformer.transform(leapFrameMultinomial).get
    val predictorResult = predictorTransformer.transform(leapFrameMultinomial).get

    probabilityColumnEqualityTest(xgboost4jResult, predictorResult)
  }

  it("XGBoost4j and Predictor results are the same when using a dense dataset") {
    val xgboost4jTransformer = trainXGBoost4jClassifier

    val mleapBundle = serializeModelToMleapBundle(xgboost4jTransformer)
    val predictorTransformer: Transformer = loadXGBoostPredictorFromBundle(mleapBundle)

    val denseLeapFrame = toDenseFeaturesLeapFrame(leapFrameBinomial)

    val xgboost4jResult = xgboost4jTransformer.transform(denseLeapFrame).get
    val predictorResult = predictorTransformer.transform(denseLeapFrame).get

    probabilityColumnEqualityTest(xgboost4jResult, predictorResult)
  }
}
