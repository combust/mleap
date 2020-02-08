package ml.combust.mleap.xgboost.runtime

import ml.combust.mleap.core.types.{BasicType, NodeShape, ScalarType, StructField, TensorType}
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Transformer}
import ml.combust.mleap.tensor.SparseTensor
import ml.combust.mleap.xgboost.runtime.testing.{BoosterUtils, BundleSerializationUtils, CachedDatasetUtils, FloatingPointApproximations}
import ml.dmlc.xgboost4j.scala.Booster
import org.apache.spark.ml.linalg.Vectors
import org.scalatest.FunSpec


class XGBoostClassificationModelParitySpec extends FunSpec
  with BoosterUtils
  with CachedDatasetUtils
  with BundleSerializationUtils
  with FloatingPointApproximations {

  def createBoosterClassifier(booster: Booster): Transformer ={

    XGBoostClassification(
      "xgboostSingleThread",
      NodeShape.probabilisticClassifier(
        rawPredictionCol = Some("raw_prediction"),
        probabilityCol = Some("probability")),
      XGBoostClassificationModel(XGBoostBinaryClassificationModel(booster, numFeatures, 0))
    )
  }

  def equalityTestRowByRow(booster: Booster, mleapTransformer: Transformer) = {

    import XgbConverters._

    leapFrameLibSVMtest.dataset.foreach {
      r=>
        val mleapResult = mleapTransformer.transform(DefaultLeapFrame(mleapSchema.get, Seq(r))).get

        val mleapPredictionColIndex = mleapResult.schema.indexOf("prediction").get
        val mleapRawPredictionColIndex = mleapResult.schema.indexOf("raw_prediction").get
        val mleapProbabilityColIndex = mleapResult.schema.indexOf("probability").get

        val singleRowDMatrix = r(1).asInstanceOf[SparseTensor[Double]].asXGB

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

  it("Results between the XGBoost4j booster and the MLeap Transformer are the same") {
    val booster = trainBooster(xgboostParams, denseDataset)
    val xgboostTransformer = createBoosterClassifier(trainBooster(xgboostParams, denseDataset))

    val mleapBundle = serializeModelToMleapBundle(xgboostTransformer)
    val deserializedTransformer: Transformer = loadMleapTransformerFromBundle(mleapBundle)

    equalityTestRowByRow(booster, deserializedTransformer)
  }

  it("has the correct inputs and outputs with columns: prediction, probability and raw_prediction") {

    val booster = trainBooster(xgboostParams, denseDataset)
    val transformer = createBoosterClassifier(booster)

    assert(transformer.schema.fields ==
      Seq(StructField("features", TensorType(BasicType.Double, Seq(numFeatures))),
        StructField("raw_prediction", TensorType(BasicType.Double, Seq(2))),
        StructField("probability", TensorType(BasicType.Double, Seq(2))),
        StructField("prediction", ScalarType.Double.nonNullable)))
  }

  it("Results are the same pre and post serialization") {
    val booster = trainBooster(xgboostParams, denseDataset)
    val xgboostTransformer = createBoosterClassifier(booster)

    val preSerializationResult = xgboostTransformer.transform(leapFrameLibSVMtrain)

    val mleapBundle = serializeModelToMleapBundle(xgboostTransformer)

    val deserializedTransformer: Transformer = loadMleapTransformerFromBundle(mleapBundle)
    val deserializedModelResult = deserializedTransformer.transform(leapFrameLibSVMtrain).get

    assert(preSerializationResult.get.dataset == deserializedModelResult.dataset)
  }
}
