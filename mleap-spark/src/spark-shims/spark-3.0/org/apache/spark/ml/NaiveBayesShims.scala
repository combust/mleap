package org.apache.spark.ml

import org.apache.spark.ml.classification.NaiveBayesModel
import org.apache.spark.ml.linalg.{Matrices, Matrix, Vector}

object NaiveBayesShims {

  def createNaiveBayesModel(uid: String, pi: Vector, theta: Matrix, sigmaOpt: Option[Matrix]): NaiveBayesModel = {
    val sigma = sigmaOpt.getOrElse(Matrices.zeros(0, 0))
    new NaiveBayesModel(uid = uid, pi = pi, theta = theta, sigma = sigma)
  }

  def getNaiveBayesModelSigma(model: NaiveBayesModel): Option[Matrix] = {
    Some(model.sigma)
  }

}
