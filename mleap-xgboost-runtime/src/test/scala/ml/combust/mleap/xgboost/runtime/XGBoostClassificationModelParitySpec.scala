package ml.combust.mleap.xgboost.runtime

import ml.combust.mleap.core.types.{BasicType, NodeShape, ScalarType, StructField, TensorType}
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Transformer}
import ml.combust.mleap.tensor.{SparseTensor, Tensor}
import ml.combust.mleap.xgboost.runtime.testing.{BoosterUtils, BundleSerializationUtils, CachedDatasetUtils, ClassifierUtils, FloatingPointApproximations}
import ml.dmlc.xgboost4j.scala.Booster
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec
import XgbConverters._


class XGBoostClassificationModelParitySpec extends FunSpec
  with BoosterUtils
  with CachedDatasetUtils
  with ClassifierUtils
  with BundleSerializationUtils
  with FloatingPointApproximations {

  def equalityTestRowByRow(
                            booster: Booster,
                            mleapTransformer: Transformer,
                            leapFrameDataset: DefaultLeapFrame) = {

    val featuresColumnIndex = leapFrameDataset.schema.indexOf("features").get

    leapFrameDataset.dataset.foreach {
      r=>
        val mleapResult = mleapTransformer.transform(DefaultLeapFrame(leapFrameDataset.schema, Seq(r))).get

        val mleapPredictionColIndex = mleapResult.schema.indexOf("prediction").get
        val mleapRawPredictionColIndex = mleapResult.schema.indexOf("raw_prediction").get
        val mleapProbabilityColIndex = mleapResult.schema.indexOf("probability").get

        val singleRowDMatrix = r(featuresColumnIndex).asInstanceOf[Tensor[Double]].asXGB

        val boosterResult = booster.predict(singleRowDMatrix, false, 0).head(0)

        val boosterProbability = Vectors.dense(1 - boosterResult, boosterResult).toDense
        val boosterPrediction = Math.round(boosterResult)


        assert (boosterPrediction == mleapResult.dataset.head.getDouble(mleapPredictionColIndex))

        assert (
          almostEqualSequences(
            Seq(boosterProbability.values),
            Seq(mleapResult.dataset.head.getTensor[Double](mleapProbabilityColIndex).toArray)
          )
        )

        val boosterResultWithMargin = booster.predict(singleRowDMatrix, true, 0).head(0)
        val boosterRawPrediction = Vectors.dense(- boosterResultWithMargin, boosterResultWithMargin).toDense

        assert (almostEqualSequences(
          Seq(boosterRawPrediction.values),
          Seq(mleapResult.dataset.head.getTensor[Double](mleapRawPredictionColIndex).toArray)
        ))
    }
  }

  def equalityTestRowByRowMultinomial(
      booster: Booster,
      mleapTransformer: Transformer,
      leapFrameDataset: DefaultLeapFrame) = {

    val featuresColumnIndex = leapFrameDataset.schema.indexOf("features").get

    leapFrameDataset.dataset.foreach {
      r =>
        val mleapResult = mleapTransformer.transform(DefaultLeapFrame(leapFrameDataset.schema, Seq(r))).get

        val mleapPredictionColIndex = mleapResult.schema.indexOf("prediction").get
        val mleapRawPredictionColIndex = mleapResult.schema.indexOf("raw_prediction").get
        val mleapProbabilityColIndex = mleapResult.schema.indexOf("probability").get

        val singleRowDMatrix = r(featuresColumnIndex).asInstanceOf[SparseTensor[Double]].asXGB

        val boosterResult = booster.predict(singleRowDMatrix, false, 0).head

        val boosterProbability = Vectors.dense(boosterResult.map(_.toDouble)).toDense
        val boosterPrediction = boosterProbability.argmax

        assert(boosterPrediction == mleapResult.dataset.head.getDouble(mleapPredictionColIndex))

        assert(
          almostEqualSequences(
            Seq(boosterProbability.values),
            Seq(mleapResult.dataset.head.getTensor[Double](mleapProbabilityColIndex).toArray)
          )
        )

        val boosterResultWithMargin = booster.predict(singleRowDMatrix, true, 0).head
        val boosterRawPrediction = Vectors.dense(boosterResultWithMargin.map(_.toDouble)).toDense

        assert(almostEqualSequences(
          Seq(boosterRawPrediction.values),
          Seq(mleapResult.dataset.head.getTensor[Double](mleapRawPredictionColIndex).toArray)
        ))
    }
  }

  it("Results between the XGBoost4j booster and the MLeap Transformer are the same") {
    val booster = trainBooster(binomialDataset)
    val xgboostTransformer = trainXGBoost4jClassifier

    equalityTestRowByRow(booster, xgboostTransformer, leapFrameBinomial)
  }

  it("has the correct inputs and outputs with columns: prediction, probability and raw_prediction") {
    val transformer = trainXGBoost4jClassifier
    val numFeatures = transformer.asInstanceOf[XGBoostClassification].model.numFeatures

    assert(transformer.schema.fields ==
      Seq(StructField("features", TensorType(BasicType.Double, Seq(numFeatures))),
        StructField("raw_prediction", TensorType(BasicType.Double, Seq(2))),
        StructField("probability", TensorType(BasicType.Double, Seq(2))),
        StructField("prediction", ScalarType.Double.nonNullable)))
  }

  it("Results are the same pre and post serialization") {
    val xgboostTransformer = trainXGBoost4jClassifier

    val mleapBundle = serializeModelToMleapBundle(xgboostTransformer)
    val deserializedTransformer: Transformer = loadMleapTransformerFromBundle(mleapBundle)

    val preSerializationResult = xgboostTransformer.transform(leapFrameBinomial).get
    val deserializedModelResult = deserializedTransformer.transform(leapFrameBinomial).get

    assert(preSerializationResult.dataset == deserializedModelResult.dataset)
  }

  it("Results between the XGBoost4j multinomial booster and the MLeap XGBoostMultinomialClassificationModel are the same") {
    val multiBooster = trainMultinomialBooster(multinomialDataset)
    val xgboostTransformer = trainMultinomialXGBoost4jClassifier

    equalityTestRowByRowMultinomial(multiBooster, xgboostTransformer, leapFrameMultinomial)
  }

  it("XGBoostMultinomialClassificationModel results are the same pre and post serialization") {
    val xgboostTransformer = trainMultinomialXGBoost4jClassifier

    val mleapBundle = serializeModelToMleapBundle(xgboostTransformer)
    val deserializedTransformer: Transformer = loadMleapTransformerFromBundle(mleapBundle)

    val preSerializationResult = xgboostTransformer.transform(leapFrameMultinomial).get
    val deserializedResult = deserializedTransformer.transform(leapFrameMultinomial).get

    assert(preSerializationResult.dataset == deserializedResult.dataset)
  }

  it("Results pre and post serialization are the same when using a dense dataset") {
    val xgboostTransformer = trainXGBoost4jClassifier

    val mleapBundle = serializeModelToMleapBundle(xgboostTransformer)
    val deserializedTransformer: Transformer = loadMleapTransformerFromBundle(mleapBundle)

    val denseLeapFrame = toDenseFeaturesLeapFrame(leapFrameBinomial)

    val preSerializationResult = xgboostTransformer.transform(denseLeapFrame).get
    val deserializedResult = deserializedTransformer.transform(denseLeapFrame).get

    assert(preSerializationResult.dataset == deserializedResult.dataset)
  }
}
