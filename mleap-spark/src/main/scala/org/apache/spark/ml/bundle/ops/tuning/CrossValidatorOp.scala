package org.apache.spark.ml.bundle.ops.tuning

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl.{Bundle, Model, Node, NodeShape}
import ml.combust.bundle.op.{OpModel, OpNode}
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.tuning.CrossValidatorModel

class CrossValidatorOp extends OpNode[SparkBundleContext, CrossValidatorModel, CrossValidatorModel] {
  override val Model: OpModel[SparkBundleContext, CrossValidatorModel] = new OpModel[SparkBundleContext, CrossValidatorModel] {
    override val klazz: Class[CrossValidatorModel] = classOf[CrossValidatorModel]

    override def opName: String = Bundle.BuiltinOps.tuning.cross_validator
    override def modelOpName(obj: CrossValidatorModel)
                            (implicit context: BundleContext[SparkBundleContext]): String = {
      context.bundleRegistry.opForObj[SparkBundleContext, Any, Any](obj.bestModel).Model.modelOpName(obj)
    }

    override def store(model: Model, obj: CrossValidatorModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      context.bundleRegistry.opForObj[SparkBundleContext, Any, Any](obj.bestModel).Model.store(model, obj.bestModel)
    }

    // Not implemented for tuning models
    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): CrossValidatorModel = ???
  }

  override val klazz: Class[CrossValidatorModel] = classOf[CrossValidatorModel]

  override def name(node: CrossValidatorModel): String = node.uid

  override def model(node: CrossValidatorModel): CrossValidatorModel = node

  override def shape(node: CrossValidatorModel)
                    (implicit context: BundleContext[SparkBundleContext]): NodeShape = {
    context.bundleRegistry.opForObj[SparkBundleContext, Any, Any](node.bestModel).shape(node.bestModel)
  }

  // Not implemented for tuning models
  override def load(node: Node, model: CrossValidatorModel)
                   (implicit context: BundleContext[SparkBundleContext]): CrossValidatorModel = ???
}
