package org.apache.spark.ml.bundle.extension.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.core.feature.{MathUnaryModel, UnaryOperation}
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.mleap.feature.MathUnary

/**
  * Created by hollinwilkins on 12/27/16.
  */
class MathUnaryOp extends OpNode[SparkBundleContext, MathUnary, MathUnaryModel] {
  override val Model: OpModel[SparkBundleContext, MathUnaryModel] = new OpModel[SparkBundleContext, MathUnaryModel] {
    override val klazz: Class[MathUnaryModel] = classOf[MathUnaryModel]

    override def opName: String = Bundle.BuiltinOps.feature.math_unary

    override def store(model: Model, obj: MathUnaryModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      model.withValue("operation", Value.string(obj.operation.name))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): MathUnaryModel = {
      MathUnaryModel(UnaryOperation.forName(model.value("operation").getString))
    }
  }

  override val klazz: Class[MathUnary] = classOf[MathUnary]

  override def name(node: MathUnary): String = node.uid

  override def model(node: MathUnary): MathUnaryModel = node.model

  override def load(node: Node, model: MathUnaryModel)
                   (implicit context: BundleContext[SparkBundleContext]): MathUnary = {
    new MathUnary(uid = node.name,
      model = model).setInputCol(node.shape.standardInput.name).
      setOutputCol(node.shape.standardOutput.name)
  }

  override def shape(node: MathUnary)(implicit context: BundleContext[SparkBundleContext]): NodeShape =
    NodeShape().withStandardIO(node.getInputCol, node.getOutputCol)
}
