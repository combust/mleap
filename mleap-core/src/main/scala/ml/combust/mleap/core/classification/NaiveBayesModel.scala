package ml.combust.mleap.core.classification

import org.apache.spark.ml.linalg.mleap.BLAS
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors, DenseMatrix}


/**
  * Created by fshabbir on 12/19/16.
  * https://github.com/apache/spark/blob/master/mllib/src/main/scala/org/apache/spark/ml/classification/NaiveBayes.scala
  */

/** Companion object for constructing NaiveBayesModel.
  */

object NaiveBayesModel {
  def apply(numFeatures: Int,
            numClasses: Int,
            w: Option[Array[Double]]) : NaiveBayesModel = {
    val weight = w match {
      case Some(w) => w
      case None => Array.fill[Double](numFeatures)(1.0)
    }
    NaiveBayesModel(numFeatures,
      numClasses,
      new Array[Double](numClasses),
      new Array[Double](numFeatures * numClasses))
  }
}

/**
  *
  * @param numFeatures number of features in feature vector
  * @param numClasses number of labels or labels to classify predictions into
  * @param piArray log of class priors
  * @param thetaArray log of class conditional probabilities
  */
case class NaiveBayesModel (numFeatures: Int,
                            numClasses:Int,
                            piArray: Array[Double],
                            thetaArray: Array[Double])
  extends MultinomialClassificationModel with Serializable {

  override def predictRaw(raw: Vector): Vector = {
    val pi = Vectors.dense(piArray)
    val theta = new DenseMatrix(numClasses, numFeatures, thetaArray, true)
    val prob = theta.multiply(raw)
    BLAS.axpy(1.0, pi, prob)
    prob
  }

  override def rawToProbabilityInPlace(raw: Vector): Vector = {
    raw match {
      case dv: DenseVector =>
        var i = 0
        val size = dv.size
        val maxLog = dv.values.max
        while (i < size) {
          dv.values(i) = math.exp(dv.values(i) - maxLog)
          i += 1
        }
        MultinomialClassificationModel.normalizeToProbabilitiesInPlace(dv)
        dv
      case sv: SparseVector =>
        throw new RuntimeException("Unexpected error in NaiveBayesModel:" +
          " raw2probabilityInPlace encountered SparseVector")
    }

  }
}
