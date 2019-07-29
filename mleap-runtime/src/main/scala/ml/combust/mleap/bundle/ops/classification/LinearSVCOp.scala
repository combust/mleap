package ml.combust.mleap.bundle.ops.classification

import ml.combust.bundle.BundleContext
import ml.combust.bundle.op.OpModel
import ml.combust.bundle.dsl._
import ml.combust.mleap.bundle.ops.MleapOp
import ml.combust.mleap.core.classification.LinearSVCModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.classification.LinearSVC
import org.apache.spark.ml.linalg.Vectors

class LinearSVCOp extends MleapOp[LinearSVC, LinearSVCModel]
{
    /** Type class for the underlying model.
      */
    override val Model: OpModel[MleapContext, LinearSVCModel] = new OpModel[MleapContext, LinearSVCModel]
    {
        override val klazz: Class[LinearSVCModel] = classOf[LinearSVCModel]

        override def opName: String = Bundle.BuiltinOps.classification.linear_svc

        override def store(model: Model, obj: LinearSVCModel)
                          (implicit context: BundleContext[MleapContext]): Model =
        {
            model
              .withValue("num_classes", Value.long(obj.numClasses))
              .withValue("coefficients", Value.vector(obj.coefficients.toArray))
              .withValue("intercept", Value.double(obj.intercept))
              .withValue("threshold", Value.double(obj.threshold))
        }

        override def load(model: Model)
                         (implicit context: BundleContext[MleapContext]): LinearSVCModel =
        {
            val numClasses = model.value("num_classes").getLong.toInt
            if(numClasses != 2)
                throw new IllegalArgumentException("Spark only supports binary SVM and so as Mleap")

            LinearSVCModel(
                coefficients = Vectors.dense(model.value("coefficients").getTensor[Double].toArray),
                    intercept =  model.value("intercept").getDouble,
                    threshold = model.value("threshold").getDouble
            )
        }
    }

    /** Get the underlying model of the node.
      *
      * @param node node object
      * @return underlying model object
      */
    override def model(node: LinearSVC): LinearSVCModel = node.model
}
