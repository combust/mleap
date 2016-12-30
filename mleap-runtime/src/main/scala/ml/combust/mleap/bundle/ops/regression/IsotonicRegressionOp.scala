package ml.combust.mleap.bundle.ops.regression

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.core.regression.IsotonicRegressionModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.regression.IsotonicRegression

/**
  * Created by hollinwilkins on 12/27/16.
  */
class IsotonicRegressionOp extends OpNode[MleapContext, IsotonicRegression, IsotonicRegressionModel] {
  override val Model: OpModel[MleapContext, IsotonicRegressionModel] = new OpModel[MleapContext, IsotonicRegressionModel] {
    override val klazz: Class[IsotonicRegressionModel] = classOf[IsotonicRegressionModel]

    override def opName: String = Bundle.BuiltinOps.regression.isotonic_regression

    override def store(model: Model, obj: IsotonicRegressionModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      model.withAttr("boundaries", Value.doubleList(obj.boundaries.toSeq)).
        withAttr("predictions", Value.doubleList(obj.predictions)).
        withAttr("isotonic", Value.boolean(obj.isotonic)).
        withAttr("feature_index", obj.featureIndex.map(v => Value.long(v)))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): IsotonicRegressionModel = {
      IsotonicRegressionModel(boundaries = model.value("boundaries").getDoubleList.toArray,
        predictions = model.value("predictions").getDoubleList,
        isotonic = model.value("isotonic").getBoolean,
        featureIndex = model.getValue("feature_index").map(_.getLong.toInt))
    }
  }

  override val klazz: Class[IsotonicRegression] = classOf[IsotonicRegression]

  override def name(node: IsotonicRegression): String = node.uid

  override def model(node: IsotonicRegression): IsotonicRegressionModel = node.model

  override def load(node: Node, model: IsotonicRegressionModel)
                   (implicit context: BundleContext[MleapContext]): IsotonicRegression = {
    IsotonicRegression(uid = node.name,
      featuresCol = node.shape.input("features").name,
      predictionCol = node.shape.output("prediction").name,
      model = model)
  }

  override def shape(node: IsotonicRegression): Shape = Shape().withInput(node.featuresCol, "features").
    withOutput(node.predictionCol, "prediction")
}
