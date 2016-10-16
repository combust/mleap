package org.apache.spark.ml.bundle

import ml.combust.bundle.serializer.BundleRegistry

/**
  * Created by hollinwilkins on 8/21/16.
  */
object SparkRegistry {
  implicit val defaultRegistry: BundleRegistry = create()

  def create(): BundleRegistry = {
    BundleRegistry().
      // regressions
      register(ops.regression.LinearRegressionOp).
      register(ops.regression.DecisionTreeRegressionOp).
      register(ops.regression.RandomForestRegressionOp).
      register(ops.regression.GBTRegressionOp).

      // classifiers
      register(ops.classification.LogisticRegressionOp).
      register(ops.classification.SupportVectorMachineOp).
      register(ops.classification.DecisionTreeClassifierOp).
      register(ops.classification.RandomForestClassifierOp).
      register(ops.classification.OneVsRestOp).
      register(ops.classification.GBTClassifierOp).

      // features
      register(ops.feature.HashingTermFrequencyOp).
      register(ops.feature.OneHotEncoderOp).
      register(ops.feature.ReverseStringIndexerOp).
      register(ops.feature.StandardScalerOp).
      register(ops.feature.StringIndexerOp).
      register(ops.feature.TokenizerOp).
      register(ops.feature.VectorAssemblerOp).
      register(ops.feature.MinMaxScalerOp).
      register(ops.feature.MaxAbsScalerOp).
      register(ops.feature.BucketizerOp).
      register(ops.feature.ElementwiseProductOp).
      register(ops.feature.PcaOp).
      register(ops.feature.NGramOp).
      register(ops.feature.StopWordsRemoverOp).

      // other
      register(ops.PipelineOp)
  }
}
