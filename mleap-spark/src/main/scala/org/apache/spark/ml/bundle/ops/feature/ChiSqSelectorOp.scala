package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.core.types.TensorShape
import org.apache.spark.ml.bundle.{ParamSpec, SimpleParamSpec, SimpleSparkOp, SparkBundleContext}
import org.apache.spark.ml.feature.ChiSqSelectorModel
import org.apache.spark.mllib.feature
import org.apache.spark.sql.mleap.TypeConverters.sparkToMleapDataShape

/**
  * Created by hollinwilkins on 12/27/16.
  */
class ChiSqSelectorOp extends SimpleSparkOp[ChiSqSelectorModel] {
  override val Model: OpModel[SparkBundleContext, ChiSqSelectorModel] = new OpModel[SparkBundleContext, ChiSqSelectorModel] {
    override val klazz: Class[ChiSqSelectorModel] = classOf[ChiSqSelectorModel]

    override def opName: String = Bundle.BuiltinOps.feature.chi_sq_selector

    override def store(model: Model, obj: ChiSqSelectorModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {

      val dataset = context.context.dataset.get
      val inputShape = sparkToMleapDataShape(dataset.schema(obj.getFeaturesCol)).asInstanceOf[TensorShape]

      model.withValue("filter_indices", Value.longList(obj.selectedFeatures.map(_.toLong).toSeq))
           .withValue("input_size", Value.int(inputShape.dimensions.get(0)))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): ChiSqSelectorModel = {
      new ChiSqSelectorModel(uid = "",
        chiSqSelector = new feature.ChiSqSelectorModel(model.value("filter_indices").getLongList.map(_.toInt).toArray))
    }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: ChiSqSelectorModel): ChiSqSelectorModel = {
    new ChiSqSelectorModel(uid = uid,
      chiSqSelector = new feature.ChiSqSelectorModel(model.selectedFeatures))
  }

  override def sparkInputs(obj: ChiSqSelectorModel): Seq[ParamSpec] = {
    Seq("input" -> obj.featuresCol)
  }

  override def sparkOutputs(obj: ChiSqSelectorModel): Seq[SimpleParamSpec] = {
    Seq("output" -> obj.outputCol)
  }
}
