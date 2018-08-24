package org.apache.spark.ml.bundle.ops.tuning

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl.{Bundle, Model, Node, NodeShape}
import ml.combust.bundle.op.{OpModel, OpNode}
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.tuning.TrainValidationSplitModel

class TrainValidationSplitOp extends OpNode[SparkBundleContext, TrainValidationSplitModel, TrainValidationSplitModel] {
  override val Model: OpModel[SparkBundleContext, TrainValidationSplitModel] = new OpModel[SparkBundleContext, TrainValidationSplitModel] {
    override val klazz: Class[TrainValidationSplitModel] = classOf[TrainValidationSplitModel]

    override def opName: String = Bundle.BuiltinOps.tuning.train_validation_split
    override def modelOpName(obj: TrainValidationSplitModel)
                            (implicit context: BundleContext[SparkBundleContext]): String = {
      context.bundleRegistry.opForObj[SparkBundleContext, Any, Any](obj.bestModel).Model.modelOpName(obj.bestModel)
    }

    override def store(model: Model, obj: TrainValidationSplitModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      context.bundleRegistry.opForObj[SparkBundleContext, Any, Any](obj.bestModel).Model.store(model, obj.bestModel)

    }

    // Not implemented for tuning models
    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): TrainValidationSplitModel = ???
  }

  override val klazz: Class[TrainValidationSplitModel] = classOf[TrainValidationSplitModel]

  override def name(node: TrainValidationSplitModel): String = node.uid

  override def model(node: TrainValidationSplitModel): TrainValidationSplitModel = node

  override def shape(node: TrainValidationSplitModel)
                    (implicit context: BundleContext[SparkBundleContext]): NodeShape = {
    context.bundleRegistry.opForObj[SparkBundleContext, Any, Any](node.bestModel).shape(node.bestModel)
  }

  // Not implemented for tuning models
  override def load(node: Node, model: TrainValidationSplitModel)
                   (implicit context: BundleContext[SparkBundleContext]): TrainValidationSplitModel = ???
}
