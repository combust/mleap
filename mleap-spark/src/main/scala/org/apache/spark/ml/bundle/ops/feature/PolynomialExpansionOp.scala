package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.core.types.TensorShape
import org.apache.spark.ml.bundle.{ParamSpec, SimpleParamSpec, SimpleSparkOp, SparkBundleContext}
import org.apache.spark.ml.feature.PolynomialExpansion
import org.apache.spark.sql.mleap.TypeConverters.sparkToMleapDataShape

/**
  * Created by mikhail on 10/16/16.
  */
class PolynomialExpansionOp extends SimpleSparkOp[PolynomialExpansion] {
  override val Model: OpModel[SparkBundleContext, PolynomialExpansion] = new OpModel[SparkBundleContext, PolynomialExpansion] {
    override val klazz: Class[PolynomialExpansion] = classOf[PolynomialExpansion]

    override def opName: String = Bundle.BuiltinOps.feature.polynomial_expansion

    override def store(model: Model, obj: PolynomialExpansion)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      val dataset = context.context.dataset.get
      val inputShape = sparkToMleapDataShape(dataset.schema(obj.getInputCol), dataset).asInstanceOf[TensorShape]
      model.withValue("degree", Value.long(obj.getDegree))
        .withValue("input_size", Value.long(inputShape.dimensions.get.head))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): PolynomialExpansion = {
      new PolynomialExpansion(uid = "").setDegree(model.value("degree").getLong.toInt)
    }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: PolynomialExpansion): PolynomialExpansion = {
    new PolynomialExpansion(uid = uid)
  }

  override def sparkInputs(obj: PolynomialExpansion): Seq[ParamSpec] = {
    Seq("input" -> obj.inputCol)
  }

  override def sparkOutputs(obj: PolynomialExpansion): Seq[SimpleParamSpec] = {
    Seq("output" -> obj.outputCol)
  }
}
