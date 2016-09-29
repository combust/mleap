package ml.combust.mleap.runtime.serialization.bundle

import ml.combust.bundle.serializer.BundleRegistry

/**
  * Created by hollinwilkins on 8/22/16.
  */
object MleapRegistry {
  implicit val defaultRegistry: BundleRegistry = create()

  def create(): BundleRegistry = BundleRegistry().
    // classification
    register(ops.classification.OneVsRestOp).
    register(ops.classification.SupportVectorMachineOp).
    register(ops.classification.DecisionTreeClassifierOp).
    register(ops.classification.RandomForestClassifierOp).
    register(ops.classification.LogisticRegressionOp).
    register(ops.classification.OneVsRestOp).
    register(ops.classification.GBTClassifierOp).

    // feature
    register(ops.feature.StandardScalerOp).
    register(ops.feature.StringIndexerOp).
    register(ops.feature.ReverseStringIndexerOp).
    register(ops.feature.VectorAssemblerOp).
    register(ops.feature.MinMaxScalerOp).
    register(ops.feature.MaxAbsScalerOp).
    register(ops.feature.BucketizerOp).
    register(ops.feature.ElementwiseProductOp).
    register(ops.feature.NormalizerOp).

    // regression
    register(ops.regression.LinearRegressionOp).
    register(ops.regression.DecisionTreeRegressionOp).
    register(ops.regression.RandomForestRegressionOp).
    register(ops.regression.GBTRegressionOp).

    // other
    register(ops.PipelineOp)
}
