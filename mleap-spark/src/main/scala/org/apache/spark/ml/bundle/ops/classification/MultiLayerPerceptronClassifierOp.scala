package org.apache.spark.ml.bundle.ops.classification

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.runtime.MleapContext
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by hollinwilkins on 12/25/16.
  */
class MultiLayerPerceptronClassifierOp extends OpNode[MleapContext, MultilayerPerceptronClassificationModel, MultilayerPerceptronClassificationModel] {
  override val Model: OpModel[MleapContext, MultilayerPerceptronClassificationModel] = new OpModel[MleapContext, MultilayerPerceptronClassificationModel] {
    override def opName: String = Bundle.BuiltinOps.classification.multi_layer_perceptron_classifier

    override val klazz: Class[MultilayerPerceptronClassificationModel] = classOf[MultilayerPerceptronClassificationModel]

    override def store(model: Model, obj: MultilayerPerceptronClassificationModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withAttr("layers", Value.longList(obj.layers.map(_.toLong))).
        withAttr("weights", Value.vector(obj.weights.toArray))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): MultilayerPerceptronClassificationModel = {
      new MultilayerPerceptronClassificationModel(uid = "",
        layers = model.value("layers").getLongList.map(_.toInt).toArray,
        weights = Vectors.dense(model.value("weights").getTensor[Double].toArray))
    }
  }

  override val klazz: Class[MultilayerPerceptronClassificationModel] = classOf[MultilayerPerceptronClassificationModel]

  override def name(node: MultilayerPerceptronClassificationModel): String = node.uid

  override def model(node: MultilayerPerceptronClassificationModel): MultilayerPerceptronClassificationModel = node

  override def load(node: Node, model: MultilayerPerceptronClassificationModel)
                   (implicit context: BundleContext[MleapContext]): MultilayerPerceptronClassificationModel = {
    new MultilayerPerceptronClassificationModel(uid = node.name,
      layers = model.layers,
      weights = model.weights).setFeaturesCol(node.shape.input("features").name).
      setPredictionCol(node.shape.output("prediction").name)
  }

  override def shape(node: MultilayerPerceptronClassificationModel): Shape = Shape().withInput(node.getFeaturesCol, "features").
    withOutput(node.getPredictionCol, "prediction")
}

