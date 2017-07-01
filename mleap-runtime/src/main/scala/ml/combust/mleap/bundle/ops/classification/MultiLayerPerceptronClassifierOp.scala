package ml.combust.mleap.bundle.ops.classification

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.core.classification.MultiLayerPerceptronClassifierModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.classification.MultiLayerPerceptronClassifier
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by hollinwilkins on 12/25/16.
  */
class MultiLayerPerceptronClassifierOp extends OpNode[MleapContext, MultiLayerPerceptronClassifier, MultiLayerPerceptronClassifierModel] {
  override val Model: OpModel[MleapContext, MultiLayerPerceptronClassifierModel] = new OpModel[MleapContext, MultiLayerPerceptronClassifierModel] {
    override def opName: String = Bundle.BuiltinOps.classification.multi_layer_perceptron_classifier

    override val klazz: Class[MultiLayerPerceptronClassifierModel] = classOf[MultiLayerPerceptronClassifierModel]

    override def store(model: Model, obj: MultiLayerPerceptronClassifierModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withAttr("layers", Value.longList(obj.layers.map(_.toLong))).
        withAttr("weights", Value.vector(obj.weights.toArray))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): MultiLayerPerceptronClassifierModel = {
      MultiLayerPerceptronClassifierModel(layers = model.value("layers").getLongList.map(_.toInt),
        weights = Vectors.dense(model.value("weights").getTensor[Double].toArray))
    }
  }

  override val klazz: Class[MultiLayerPerceptronClassifier] = classOf[MultiLayerPerceptronClassifier]

  override def name(node: MultiLayerPerceptronClassifier): String = node.uid

  override def model(node: MultiLayerPerceptronClassifier): MultiLayerPerceptronClassifierModel = node.model

  override def load(node: Node, model: MultiLayerPerceptronClassifierModel)
                   (implicit context: BundleContext[MleapContext]): MultiLayerPerceptronClassifier = {
    MultiLayerPerceptronClassifier(uid = node.name,
      featuresCol = node.shape.input("features").name,
      predictionCol = node.shape.output("prediction").name,
      model = model)
  }

  override def shape(node: MultiLayerPerceptronClassifier): NodeShape = NodeShape().withInput(node.featuresCol, "features").
    withOutput(node.predictionCol, "prediction")
}
