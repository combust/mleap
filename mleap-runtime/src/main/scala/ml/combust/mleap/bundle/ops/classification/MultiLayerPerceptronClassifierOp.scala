package ml.combust.mleap.bundle.ops.classification

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.core.classification.MultiLayerPerceptronClassifierModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.classification.MultiLayerPerceptronClassifier
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by hollinwilkins on 12/25/16.
  */
class MultiLayerPerceptronClassifierOp extends MleapOp[MultiLayerPerceptronClassifier, MultiLayerPerceptronClassifierModel] {
  override val Model: OpModel[MleapContext, MultiLayerPerceptronClassifierModel] = new OpModel[MleapContext, MultiLayerPerceptronClassifierModel] {
    override def opName: String = Bundle.BuiltinOps.classification.multi_layer_perceptron_classifier

    override val klazz: Class[MultiLayerPerceptronClassifierModel] = classOf[MultiLayerPerceptronClassifierModel]

    override def store(model: Model, obj: MultiLayerPerceptronClassifierModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withValue("layers", Value.longList(obj.layers.map(_.toLong))).
        withValue("weights", Value.vector(obj.weights.toArray)).
        withValue("thresholds", obj.thresholds.map(Value.doubleList(_)))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): MultiLayerPerceptronClassifierModel = {
      MultiLayerPerceptronClassifierModel(layers = model.value("layers").getLongList.map(_.toInt),
        weights = Vectors.dense(model.value("weights").getTensor[Double].toArray),
        thresholds = model.getValue("thresholds").map(_.getDoubleList.toArray))
    }
  }

  override def model(node: MultiLayerPerceptronClassifier): MultiLayerPerceptronClassifierModel = node.model
}
