package org.apache.spark.ml.bundle.ops.regression

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.linalg.VectorUDT
import org.apache.spark.ml.regression.IsotonicRegressionModel
import org.apache.spark.mllib.regression
import org.apache.spark.sql.mleap.TypeConverters.fieldType
import org.apache.spark.sql.types.DoubleType

/**
  * Created by hollinwilkins on 12/27/16.
  */
class IsotonicRegressionOp extends OpNode[SparkBundleContext, IsotonicRegressionModel, IsotonicRegressionModel] {
  override val Model: OpModel[SparkBundleContext, IsotonicRegressionModel] = new OpModel[SparkBundleContext, IsotonicRegressionModel] {
    override val klazz: Class[IsotonicRegressionModel] = classOf[IsotonicRegressionModel]

    override def opName: String = Bundle.BuiltinOps.regression.isotonic_regression

    override def store(model: Model, obj: IsotonicRegressionModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      var m = model.withAttr("boundaries", Value.doubleList(obj.boundaries.toArray.toSeq)).
        withAttr("predictions", Value.doubleList(obj.predictions.toArray.toSeq)).
        withAttr("isotonic", Value.boolean(obj.getIsotonic))
      if(context.context.dataset.get.schema(obj.getFeaturesCol).dataType.isInstanceOf[VectorUDT]) {
        m = m.withAttr("feature_index", Value.long(obj.getFeatureIndex))
      }

      m
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): IsotonicRegressionModel = {
      val oldModel = new regression.IsotonicRegressionModel(boundaries = model.value("boundaries").getDoubleList.toArray,
        predictions = model.value("predictions").getDoubleList.toArray,
        isotonic = model.value("isotonic").getBoolean)
      val m = new IsotonicRegressionModel(uid = "",
        oldModel = oldModel)
      model.getValue("feature_index").foreach(i => m.setFeatureIndex(i.getLong.toInt))

      m
    }
  }

  override val klazz: Class[IsotonicRegressionModel] = classOf[IsotonicRegressionModel]

  override def name(node: IsotonicRegressionModel): String = node.uid

  override def model(node: IsotonicRegressionModel): IsotonicRegressionModel = node

  override def load(node: Node, model: IsotonicRegressionModel)
                   (implicit context: BundleContext[SparkBundleContext]): IsotonicRegressionModel = {
    val oldModel = new regression.IsotonicRegressionModel(boundaries = model.boundaries.toArray,
      predictions = model.predictions.toArray,
      isotonic = model.getIsotonic)
    new IsotonicRegressionModel(uid = node.name, oldModel = oldModel).
      setFeaturesCol(node.shape.input("features").name).
      setPredictionCol(node.shape.output("prediction").name).
      setFeatureIndex(model.getFeatureIndex)
  }

  override def shape(node: IsotonicRegressionModel)(implicit context: BundleContext[SparkBundleContext]): Shape = {
    val dataset = context.context.dataset
    Shape().withInput(node.getFeaturesCol, "features", fieldType(node.getFeaturesCol, dataset))
      .withOutput(node.getPredictionCol, "prediction", fieldType(node.getPredictionCol, dataset))
  }
}
