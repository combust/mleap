package ml.combust.mleap.xgboost.runtime

import biz.k11i.xgboost.Predictor
import ml.combust.mleap.core.types._
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Transformer}
import ml.combust.mleap.tensor.SparseTensor
import ml.combust.mleap.xgboost.runtime.testing.{BoosterUtils, BundleSerializationUtils, CachedDatasetUtils, ClassifierUtils, FloatingPointApproximations}
import ml.dmlc.xgboost4j.scala.Booster
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.funspec.AnyFunSpec
import XgbConverters._


class XGBoostPredictorClassificationModelParitySpec extends org.scalatest.funspec.AnyFunSpec
  with BoosterUtils
  with CachedDatasetUtils
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
    val deserializedPredictor: XGBoostPredictorClassification = ClassifierUtils.deserializedXGBoostPredictor.asInstanceOf[XGBoostPredictorClassification]
    assert(deserializedPredictor.model.impl.predictor.isInstanceOf[Predictor])
  }

  it("A pre-serialization XGBoost4j model has the same results of a deserialized Predictor model") {
    val preSerializationXGBoost4jResult = ClassifierUtils.mleapTransformer.transform(leapFrameBinomial).get
    val predictorModelResult = ClassifierUtils.deserializedXGBoostPredictor.transform(leapFrameBinomial).get
    probabilityColumnEqualityTest(preSerializationXGBoost4jResult, predictorModelResult)
  }

  it("A deserialized XGBoost4j has the same results of a deserialized Predictor"){
    val deserializedXGBoost4jResult = ClassifierUtils.deserializedmleapTransformer.transform(leapFrameBinomial).get
    val deserializedPredictorResult = ClassifierUtils.deserializedXGBoostPredictor.transform(leapFrameBinomial).get
    probabilityColumnEqualityTest(deserializedPredictorResult, deserializedXGBoost4jResult)
  }

  it("Predictor has the correct inputs and an output probability column") {
    val numFeatures = ClassifierUtils.deserializedXGBoostPredictor.asInstanceOf[XGBoostPredictorClassification].model.numFeatures
    assert(ClassifierUtils.deserializedXGBoostPredictor.schema.fields ==
      Seq(StructField("features", TensorType(BasicType.Double, Seq(numFeatures))),
        StructField("probability", TensorType(BasicType.Double, Seq(2)))))
  }

  it("[Multinomial] An XGBoost4j Booster has the same results as a deserialized Predictor"){
    equalityTestRowByRowMultinomialProbability(ClassifierUtils.multinomialBooster, ClassifierUtils.deserializedMultinomialXGBoostPredictor, leapFrameMultinomial)
  }

  it("[Multinomial] XGBoostPredictorMultinomialClassificationModel results are the same pre and post serialization") {
    val xgboost4jResult = ClassifierUtils.multinomialMleapTransformer.transform(leapFrameMultinomial).get
    val predictorResult = ClassifierUtils.deserializedMultinomialXGBoostPredictor.transform(leapFrameMultinomial).get
    probabilityColumnEqualityTest(xgboost4jResult, predictorResult)
  }

  it("XGBoost4j and Predictor results are the same when using a dense dataset") {
    val denseLeapFrame = toDenseFeaturesLeapFrame(leapFrameBinomial)
    val xgboost4jResult = ClassifierUtils.mleapTransformer.transform(denseLeapFrame).get
    val predictorResult = ClassifierUtils.deserializedXGBoostPredictor.transform(denseLeapFrame).get
    probabilityColumnEqualityTest(xgboost4jResult, predictorResult)
  }
}
