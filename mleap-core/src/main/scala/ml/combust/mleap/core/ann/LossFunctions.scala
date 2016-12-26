package ml.combust.mleap.core.ann

/** NOTE: this code was taken from Spark and adopted for
  * use by MLeap outside of a Spark Context
  *
  * https://github.com/apache/spark/blob/v2.0.0/mllib/src/main/scala/org/apache/spark/ml/ann/LossFunctions.scala
  */

import java.util.Random

import breeze.linalg.{sum => Bsum, DenseMatrix => BDM, DenseVector => BDV}
import breeze.numerics.{log => brzlog}

/**
  * Trait for loss function
  */
trait LossFunction {
  /**
    * Returns the value of loss function.
    * Computes loss based on target and output.
    * Writes delta (error) to delta in place.
    * Delta is allocated based on the outputSize
    * of model implementation.
    *
    * @param output actual output
    * @param target target output
    * @param delta delta (updated in place)
    * @return loss
    */
  def loss(output: BDM[Double], target: BDM[Double], delta: BDM[Double]): Double
}

class SigmoidLayerWithSquaredError extends Layer {
  override val weightSize = 0
  override val inPlace = true

  override def getOutputSize(inputSize: Int): Int = inputSize
  override def createModel(weights: BDV[Double]): LayerModel =
    new SigmoidLayerModelWithSquaredError()
  override def initModel(weights: BDV[Double], random: Random): LayerModel =
    new SigmoidLayerModelWithSquaredError()
}

class SigmoidLayerModelWithSquaredError
  extends FunctionalLayerModel(new FunctionalLayer(new SigmoidFunction)) with LossFunction {
  override def loss(output: BDM[Double], target: BDM[Double], delta: BDM[Double]): Double = {
    ApplyInPlace(output, target, delta, (o: Double, t: Double) => o - t)
    val error = Bsum(delta :* delta) / 2 / output.cols
    ApplyInPlace(delta, output, delta, (x: Double, o: Double) => x * (o - o * o))
    error
  }
}

class SoftmaxLayerWithCrossEntropyLoss extends Layer {
  override val weightSize = 0
  override val inPlace = true

  override def getOutputSize(inputSize: Int): Int = inputSize
  override def createModel(weights: BDV[Double]): LayerModel =
    new SoftmaxLayerModelWithCrossEntropyLoss()
  override def initModel(weights: BDV[Double], random: Random): LayerModel =
    new SoftmaxLayerModelWithCrossEntropyLoss()
}

class SoftmaxLayerModelWithCrossEntropyLoss extends LayerModel with LossFunction {

  // loss layer models do not have weights
  val weights = new BDV[Double](0)

  override def eval(data: BDM[Double], output: BDM[Double]): Unit = {
    var j = 0
    // find max value to make sure later that exponent is computable
    while (j < data.cols) {
      var i = 0
      var max = Double.MinValue
      while (i < data.rows) {
        if (data(i, j) > max) {
          max = data(i, j)
        }
        i += 1
      }
      var sum = 0.0
      i = 0
      while (i < data.rows) {
        val res = math.exp(data(i, j) - max)
        output(i, j) = res
        sum += res
        i += 1
      }
      i = 0
      while (i < data.rows) {
        output(i, j) /= sum
        i += 1
      }
      j += 1
    }
  }
  override def computePrevDelta(
                                 nextDelta: BDM[Double],
                                 input: BDM[Double],
                                 delta: BDM[Double]): Unit = {
    /* loss layer model computes delta in loss function */
  }

  override def grad(delta: BDM[Double], input: BDM[Double], cumGrad: BDV[Double]): Unit = {
    /* loss layer model does not have weights */
  }

  override def loss(output: BDM[Double], target: BDM[Double], delta: BDM[Double]): Double = {
    ApplyInPlace(output, target, delta, (o: Double, t: Double) => o - t)
    -Bsum( target :* brzlog(output)) / output.cols
  }
}
