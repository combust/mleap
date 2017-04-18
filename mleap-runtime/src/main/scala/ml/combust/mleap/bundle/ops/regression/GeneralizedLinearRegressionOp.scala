package ml.combust.mleap.bundle.ops.regression

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.core.regression.GeneralizedLinearRegressionModel
import ml.combust.mleap.core.regression.GeneralizedLinearRegressionModel.{Family, FamilyAndLink, Link}
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.regression.GeneralizedLinearRegression
import org.apache.spark.ml.linalg.Vectors

/**
  * Created by hollinwilkins on 12/28/16.
  */
class GeneralizedLinearRegressionOp extends OpNode[MleapContext, GeneralizedLinearRegression, GeneralizedLinearRegressionModel] {
  override val Model: OpModel[MleapContext, GeneralizedLinearRegressionModel] = new OpModel[MleapContext, GeneralizedLinearRegressionModel] {
    override val klazz: Class[GeneralizedLinearRegressionModel] = classOf[GeneralizedLinearRegressionModel]

    override def opName: String = Bundle.BuiltinOps.regression.generalized_linear_regression

    override def store(model: Model, obj: GeneralizedLinearRegressionModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withAttr("coefficients", Value.vector(obj.coefficients.toArray)).
        withAttr("intercept", Value.double(obj.intercept)).
        withAttr("family", Value.string(obj.fal.family.name)).
        withAttr("link", Value.string(obj.fal.link.name))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): GeneralizedLinearRegressionModel = {
      GeneralizedLinearRegressionModel(coefficients = Vectors.dense(model.value("coefficients").getTensor[Double].toArray),
        intercept = model.value("intercept").getDouble,
        fal = new FamilyAndLink(Family.fromName(model.value("family").getString),
          Link.fromName(model.value("link").getString)))
    }
  }

  override val klazz: Class[GeneralizedLinearRegression] = classOf[GeneralizedLinearRegression]

  override def name(node: GeneralizedLinearRegression): String = node.uid

  override def model(node: GeneralizedLinearRegression): GeneralizedLinearRegressionModel = node.model

  override def load(node: Node, model: GeneralizedLinearRegressionModel)
                   (implicit context: BundleContext[MleapContext]): GeneralizedLinearRegression = {
    GeneralizedLinearRegression(uid = node.name,
      featuresCol = node.shape.input("features").name,
      predictionCol = node.shape.output("prediction").name,
      linkPredictionCol = node.shape.getOutput("link_prediction").map(_.name),
      model = model)
  }

  override def shape(node: GeneralizedLinearRegression)(implicit context: BundleContext[MleapContext]): Shape = {
    Shape().withInput(node.featuresCol, "features").
      withOutput(node.predictionCol, "prediction").
      withOutput(node.linkPredictionCol, "link_prediction")
  }
}
