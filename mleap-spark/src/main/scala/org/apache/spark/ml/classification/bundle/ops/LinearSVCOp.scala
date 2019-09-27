package org.apache.spark.ml.classification.bundle.ops

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl.{Bundle, Model, NodeShape, Value}
import ml.combust.bundle.op.OpModel
import org.apache.spark.ml.bundle.{ParamSpec, SimpleParamSpec, SimpleSparkOp, SparkBundleContext}
import org.apache.spark.ml.classification.LinearSVCModel
import org.apache.spark.ml.linalg.Vectors

/**
  * Class for registering the LinearSVC model from spark.ml project.<br />
  * Note: Since org.apache.spark.ml.classification.LinearSVCModel is defined as "private[classification]", this class
  * is defined in a slightly different hierarchy.
  */
class LinearSVCOp extends SimpleSparkOp[LinearSVCModel]
{
    /** Type class for the underlying model.
      */
    override val Model: OpModel[SparkBundleContext, LinearSVCModel] = new OpModel[SparkBundleContext, LinearSVCModel]
    {
        override val klazz: Class[LinearSVCModel] = classOf[LinearSVCModel]

        override def opName: String = Bundle.BuiltinOps.classification.linear_svc

        override def store(model: Model, obj: LinearSVCModel)
                          (implicit context: BundleContext[SparkBundleContext]): Model =
        {
            val m = model.withValue("num_classes", Value.long(obj.numClasses))
            // Set the rest of the parameters
            m.withValue("coefficients", Value.vector(obj.coefficients.toArray))
                    .withValue("intercept", Value.double(obj.intercept))
                    .withValue("threshold", Value.double(obj.getThreshold))
        }

        override def load(model: Model)
                         (implicit context: BundleContext[SparkBundleContext]): LinearSVCModel =
        {
            new LinearSVCModel(uid = "",
                coefficients = Vectors.dense(model.value("coefficients").getTensor[Double].toArray),
                intercept = model.value("intercept").getDouble
              ).setThreshold(model.value("threshold").getDouble)
        }
    }

    override def sparkInputs(obj: LinearSVCModel): Seq[ParamSpec] =
    {
        Seq("features" -> obj.featuresCol)
    }

    override def sparkOutputs(obj: LinearSVCModel): Seq[SimpleParamSpec] =
    {
        Seq("raw_prediction" -> obj.rawPredictionCol,
            "prediction" -> obj.predictionCol)
    }

    override def sparkLoad(uid: String, shape: NodeShape, model: LinearSVCModel): LinearSVCModel =
    {
        new LinearSVCModel(uid = uid, coefficients = model.coefficients, intercept = model.intercept).setThreshold(model.getThreshold)
    }
}
