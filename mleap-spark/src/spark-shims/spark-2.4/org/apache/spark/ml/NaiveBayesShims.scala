package org.apache.spark.ml

import org.apache.spark.ml.classification.NaiveBayesModel
import org.apache.spark.ml.linalg.{Matrix, Vector}

object NaiveBayesShims {

  def createNaiveBayesModel(uid: String, pi: Vector, theta: Matrix, sigmaOpt: Option[Matrix]): NaiveBayesModel = {
    require(sigmaOpt.isEmpty, "spark-2.4 naive bayes model doesn't support sigma.")
    new NaiveBayesModel(uid = uid, pi = pi, theta = theta)
  }

  def getNaiveBayesModelSigma(model: NaiveBayesModel): Option[Matrix] = {
    None
  }

}
