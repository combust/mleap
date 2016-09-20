package ml.combust.mleap.runtime.serialization.bundle

import ml.bundle.serializer.BundleRegistry

/**
  * Created by hollinwilkins on 8/22/16.
  */
object MleapRegistry {
  val instance: BundleRegistry = create()

  def create(): BundleRegistry = BundleRegistry().
    // classification
    register(ops.classification.OneVsRestOp).
    register(ops.classification.SupportVectorMachineOp).
    register(ops.classification.DecisionTreeClassifierOp).
    register(ops.classification.RandomForestClassifierOp).
    register(ops.classification.LogisticRegressionOp).
    register(ops.classification.OneVsRestOp).
    // feature
    register(ops.feature.StandardScalerOp).
    register(ops.feature.StringIndexerOp).
    register(ops.feature.ReverseStringIndexerOp).
    register(ops.feature.VectorAssemblerOp).
    register(ops.feature.MinMaxScalerOp).
    register(ops.feature.MaxAbsScalerOp).
    // regression
    register(ops.regression.LinearRegressionOp).
    register(ops.regression.DecisionTreeRegressionOp).
    register(ops.regression.RandomForestRegressionOp).
    // other
    register(ops.PipelineOp)
}
