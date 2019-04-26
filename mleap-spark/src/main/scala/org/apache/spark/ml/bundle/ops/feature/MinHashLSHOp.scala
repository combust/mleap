package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import ml.combust.mleap.core.types.TensorShape
import org.apache.spark.ml.bundle.{ParamSpec, SimpleParamSpec, SimpleSparkOp, SparkBundleContext}
import org.apache.spark.ml.feature.MinHashLSHModel
import org.apache.spark.sql.mleap.TypeConverters.sparkToMleapDataShape

/**
  * Created by hollinwilkins on 12/28/16.
  */
class MinHashLSHOp extends SimpleSparkOp[MinHashLSHModel] {
  override val Model: OpModel[SparkBundleContext, MinHashLSHModel] = new OpModel[SparkBundleContext, MinHashLSHModel] {
    override val klazz: Class[MinHashLSHModel] = classOf[MinHashLSHModel]

    override def opName: String = Bundle.BuiltinOps.feature.min_hash_lsh

    override def store(model: Model, obj: MinHashLSHModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      val (ca, cb) = obj.randCoefficients.unzip

      val dataset = context.context.dataset.get
      val inputShape = sparkToMleapDataShape(dataset.schema(obj.getInputCol), dataset).asInstanceOf[TensorShape]

      model.withValue("random_coefficients_a", Value.longList(ca.map(_.toLong))).
        withValue("random_coefficients_b", Value.longList(cb.map(_.toLong)))
        .withValue("input_size", Value.int(inputShape.dimensions.get(0)))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): MinHashLSHModel = {
      val ca = model.value("random_coefficients_a").getLongList.map(_.toInt)
      val cb = model.value("random_coefficients_b").getLongList.map(_.toInt)
      val randomCoefficients = ca.zip(cb)
      new MinHashLSHModel(uid = "", randCoefficients = randomCoefficients.toArray)
    }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: MinHashLSHModel): MinHashLSHModel = {
    new MinHashLSHModel(uid = uid, randCoefficients = model.randCoefficients)
  }

  override def sparkInputs(obj: MinHashLSHModel): Seq[ParamSpec] = {
    Seq("input" -> obj.inputCol)
  }

  override def sparkOutputs(obj: MinHashLSHModel): Seq[SimpleParamSpec] = {
    Seq("output" -> obj.outputCol)
  }

  override def load(node: Node, model: MinHashLSHModel)(implicit context: BundleContext[SparkBundleContext]): MinHashLSHModel = {
    new MinHashLSHModel(uid = node.name, randCoefficients = model.randCoefficients)
  }
}
