package org.apache.spark.ml

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel

object MLPShims {

  def createMLPModel(uid: String, layers: Array[Int], weights: Vector):
      MultilayerPerceptronClassificationModel = {
    new MultilayerPerceptronClassificationModel(uid = uid, layers = layers, weights = weights)
  }

  def getMLPModelLayers(model: MultilayerPerceptronClassificationModel): Array[Int] = {
    model.layers
  }

}
