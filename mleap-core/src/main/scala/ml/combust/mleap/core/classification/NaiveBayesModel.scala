package ml.combust.mleap.core.classification

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.annotation.SparkCode
import ml.combust.mleap.core.classification.NaiveBayesModel.{Bernoulli, ModelType, Multinomial}
import org.apache.spark.ml.linalg.mleap.{BLAS, Matrices}
import org.apache.spark.ml.linalg.{DenseVector, Matrix, SparseVector, Vector}


/**
  * Created by fshabbir on 12/19/16.
  * https://github.com/apache/spark/blob/master/mllib/src/main/scala/org/apache/spark/ml/classification/NaiveBayes.scala
  */

/** Companion object for constructing NaiveBayesModel.
  */
@SparkCode(uri = "https://github.com/apache/spark/blob/master/mllib/src/main/scala/org/apache/spark/ml/classification/NaiveBayes.scala")
object NaiveBayesModel {

  def forName(name: String): ModelType = name match{
    case "bernoulli" => Bernoulli
    case "multinomial" => Multinomial
  }

  sealed trait ModelType
  case object Bernoulli extends ModelType
  case object Multinomial extends ModelType
}

/**
  *
  * @param numFeatures number of features in feature vector
  * @param numClasses number of labels or labels to classify predictions into
  * @param pi log of class priors
  * @param theta log of class conditional probabilities
  */
@SparkCode(uri = "https://github.com/apache/spark/blob/master/mllib/src/main/scala/org/apache/spark/ml/classification/NaiveBayes.scala")
case class NaiveBayesModel(numFeatures: Int,
                           numClasses: Int,
                           pi: Vector,
                           theta: Matrix,
                           modelType: NaiveBayesModel.ModelType,
                           override val thresholds: Option[Array[Double]] = None)
  extends ProbabilisticClassificationModel with Model {

  private def multinomialCalculation(raw: Vector) = {
    val prob = theta.multiply(raw)
    BLAS.axpy(1.0, pi, prob)
    prob
  }

  private def bernoulliCalculation(raw: Vector) = {
    val negTheta = Matrices.map(theta, value => math.log(1.0 - math.exp(value)))
    val ones = new DenseVector(Array.fill(theta.numCols) {1.0})
    val thetaMinusNegTheta = Matrices.map(theta, value =>
      value - math.log(1.0 - math.exp(value)))
    val negThetaSum = negTheta.multiply(ones)

    raw.foreachActive((_, value) =>
      require(value == 0.0 || value == 1.0,
        s"Bernoulli naive Bayes requires 0 or 1 feature values but found $raw.")
    )
    val prob = thetaMinusNegTheta.multiply(raw)
    BLAS.axpy(1.0, pi, prob)
    BLAS.axpy(1.0, negThetaSum, prob)
    prob
  }

  override def predictRaw(raw: Vector): Vector = {
    modelType match {
      case Multinomial =>
        multinomialCalculation(raw)
      case Bernoulli =>
        bernoulliCalculation(raw)
    }
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
        ProbabilisticClassificationModel.normalizeToProbabilitiesInPlace(dv)
        dv
      case sv: SparseVector =>
        throw new RuntimeException("Unexpected error in NaiveBayesModel:" +
          " raw2probabilityInPlace encountered SparseVector")
    }

  }
}
