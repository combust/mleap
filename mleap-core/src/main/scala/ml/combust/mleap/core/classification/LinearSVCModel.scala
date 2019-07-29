package ml.combust.mleap.core.classification

import ml.combust.mleap.core.annotation.SparkCode
import ml.combust.mleap.core.types.{ScalarType, StructType, TensorType}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.linalg.mleap.BLAS

object LinearSVCModel
{
    val defaultThreshold = 0.0
}

@SparkCode(uri = "https://github.com/apache/spark/blob/master/mllib/src/main/scala/org/apache/spark/ml/classification/LinearSVC.scala")
case class LinearSVCModel(coefficients: Vector,
                            intercept: Double,
                            threshold: Double = LinearSVCModel.defaultThreshold
                         ) extends ClassificationModel
{
    val numClasses: Int = 2
    val numFeatures: Int = coefficients.size

    private val margin: Vector => Double = features =>
    {
        BLAS.dot(features, coefficients) + intercept
    }

    /**
      * Predict class based on feature vector.
      *
      * @param features feature vector
      * @return predicted class or probability
      */
    override def predict(features: Vector): Double =
    {
        if (margin(features) > threshold) 1.0 else 0.0
    }

    override def predictRaw(features: Vector): Vector =
    {
        val m = margin(features)
        Vectors.dense(-m, m)
    }

    def rawToPrediction(rawPrediction: Vector): Double =
    {
        if (rawPrediction(1) > threshold) 1.0 else 0.0
    }

    override def inputSchema: StructType =  StructType("features" -> TensorType.Double(numFeatures)).get

    override def outputSchema: StructType = StructType("raw_prediction" -> TensorType.Double(numClasses),
        "prediction" -> ScalarType.Double.nonNullable).get
}
