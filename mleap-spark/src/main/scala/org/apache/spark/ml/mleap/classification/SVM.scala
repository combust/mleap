package org.apache.spark.ml.mleap.classification

import org.apache.spark.ml.classification.{ProbabilisticClassificationModel, ProbabilisticClassifier}
import org.apache.spark.ml.linalg
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.mllib.classification
import org.apache.spark.mllib.linalg.{BLAS, Vector, Vectors}
import org.apache.spark.mllib.optimization.{GradientDescent, HingeGradient, SquaredL2Updater}
import org.apache.spark.mllib.regression.{GeneralizedLinearAlgorithm, LabeledPoint}
import org.apache.spark.mllib.util.DataValidators
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions.col


/**
  * Created by hollinwilkins on 4/14/16.
  */
trait SVMBase extends Params {
  /**
    * Param for step size.
    * @group param
    */
  final val stepSize: DoubleParam = new DoubleParam(this, "stepSize", "step size for SGD")

  /** @group getParam */
  final def getStepSize: Double = $(stepSize)

  /**
    * Param for number of iterations.
    * @group param
    */
  final val numIterations: IntParam = new IntParam(this, "numIterations", "number of iterations for SGD")

  /** @group getParam */
  final def getNumIterations: Int = $(numIterations)

  /**
    * Param for number of iterations.
    * @group param
    */
  final val regParam: DoubleParam = new DoubleParam(this, "regParams", "regularization param for SGD")

  /** @group getParam */
  final def getRegParam: Double = $(regParam)

  /**
    * Param for number of iterations.
    * @group param
    */
  final val miniBatchFraction: DoubleParam = new DoubleParam(this, "miniBatchFraction", "Mini batch fraction for SGD")

  /** @group getParam */
  final def getMiniBatchFraction: Double = $(miniBatchFraction)

  /**
    * Param for whether to fit the intercept.
    * @group param
    */
  final val fitIntercept: BooleanParam = new BooleanParam(this, "fitIntercept", "Whether or not to fit intercept")

  /** @group getParam */
  final def getFitIntercept: Boolean = $(fitIntercept)

  /**
    * Param for threshold.
    * @group param
    */
  final val threshold: DoubleParam = new DoubleParam(this, "threshold", "Threshold")

  /** @group getParam */
  final def getThreshold: Double = $(threshold)
}

class SVMModel(override val uid: String,
               val model: classification.SVMModel) extends ProbabilisticClassificationModel[linalg.Vector, SVMModel]
  with SVMBase {
  def this(model: classification.SVMModel) = this(Identifiable.randomUID("svmModel"), model)

  def setThreshold(value: Double): this.type = {
    model.setThreshold(value)
    set(threshold, value)
  }
  setDefault(threshold, 0.5)

  def margin(features: linalg.Vector): Double = {
    BLAS.dot(model.weights, Vectors.dense(features.toArray)) + model.intercept
  }

  protected override def predict(features: linalg.Vector): Double = {
    if(margin(features)> getThreshold) 1.0 else 0.0
  }

  override protected def predictRaw(features: linalg.Vector): linalg.Vector = {
    val m = margin(features)
    linalg.Vectors.dense(-m, m)
  }

  override def numClasses: Int = 2

  override def copy(extra: ParamMap): SVMModel = defaultCopy(extra)

  override protected def raw2probabilityInPlace(rawPrediction: linalg.Vector): linalg.Vector = {
    rawPrediction match {
      case dv: linalg.DenseVector =>
        var i = 0
        val size = dv.size
        while (i < size) {
          dv.values(i) = 1.0 / (1.0 + math.exp(-dv.values(i)))
          i += 1
        }
        dv
      case sv: linalg.SparseVector =>
        throw new RuntimeException("Unexpected error in LogisticRegressionModel:" +
          " raw2probabilitiesInPlace encountered SparseVector")
    }
  }
}

class SVMWithSGD(private var stepSize: Double,
                 private var numIterations: Int,
                 private var regParam: Double,
                 private var miniBatchFraction: Double)
  extends GeneralizedLinearAlgorithm[classification.SVMModel] with Serializable {

  private val gradient = new HingeGradient()
  private val updater = new SquaredL2Updater()

  override val optimizer = new GradientDescent(gradient, updater)
    .setStepSize(stepSize)
    .setNumIterations(numIterations)
    .setRegParam(regParam)
    .setMiniBatchFraction(miniBatchFraction)
  override protected val validators = List(DataValidators.binaryLabelValidator)

  /**
    * Construct a SVM object with default parameters: {stepSize: 1.0, numIterations: 100,
    * regParm: 0.01, miniBatchFraction: 1.0}.
    */
  def this() = this(1.0, 100, 0.01, 1.0)

  override protected def createModel(weights: Vector, intercept: Double) = {
    new classification.SVMModel(weights, intercept)
  }
}

class SVM(override val uid: String)
  extends ProbabilisticClassifier[linalg.Vector, SVM, SVMModel]
    with SVMBase {
  def this() = this(Identifiable.randomUID("svm"))

  /** @group setParam */
  def setNumIterations(value: Int): this.type = set(numIterations, value)

  /** @group setParam */
  def setStepSize(value: Double): this.type = set(stepSize, value)
  setDefault(stepSize, 1.0)

  /** @group setParam */
  def setRegParam(value: Double): this.type = set(regParam, value)
  setDefault(regParam, 0.01)

  /** @group setParam */
  def setMiniBatchFraction(value: Double): this.type = set(miniBatchFraction, value)
  setDefault(miniBatchFraction, 1.0)

  /** @group setParam */
  def setFitIntercept(value: Boolean): this.type = set(fitIntercept, value)
  setDefault(fitIntercept, true)

  /** @group setParam */
  def setThreshold(value: Double): this.type = set(threshold, value)
  setDefault(threshold, 0.5)

  override def copy(extra: ParamMap): SVM = defaultCopy(extra)

  override protected def train(dataset: Dataset[_]): SVMModel = {
    val labeledPoints = dataset.select(col($(featuresCol)), col($(labelCol)))
      .rdd
      .map {
        case Row(label: Double, features: linalg.Vector) => LabeledPoint(label, Vectors.dense(features.toArray))
      }

    val mllibModel = new SVMWithSGD(getStepSize,
      getNumIterations,
      getRegParam,
      getMiniBatchFraction).
      setIntercept(getFitIntercept).
      run(labeledPoints).
      setThreshold(getThreshold)


    new SVMModel(mllibModel)
  }
}
