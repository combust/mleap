package org.apache.spark.ml

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel

object MLPShims {

  def createMLPModel(uid: String, layers: Array[Int], weights: Vector):
      MultilayerPerceptronClassificationModel = {
    val m = new MultilayerPerceptronClassificationModel(uid = uid, weights = weights)
    m.set(m.layers, layers)
    m
  }

  def getMLPModelLayers(model: MultilayerPerceptronClassificationModel): Array[Int] = {
    model.getLayers
  }

}
