package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import org.apache.spark.ml.bundle.{ParamSpec, SimpleParamSpec, SimpleSparkOp, SparkBundleContext}
import org.apache.spark.ml.feature.ElementwiseProduct
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.ml.param.Param

/**
  * Created by mikhail on 9/23/16.
  */
class ElementwiseProductOp extends SimpleSparkOp[ElementwiseProduct] {
  override val Model: OpModel[SparkBundleContext, ElementwiseProduct] = new OpModel[SparkBundleContext, ElementwiseProduct] {
    override val klazz: Class[ElementwiseProduct] = classOf[ElementwiseProduct]

    override def opName: String = Bundle.BuiltinOps.feature.elementwise_product

    override def store(model: Model, obj: ElementwiseProduct)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      model.withValue("scaling_vec", Value.vector(obj.getScalingVec.toArray))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): ElementwiseProduct = {
      new ElementwiseProduct(uid = "").setScalingVec(Vectors.dense(model.value("scaling_vec").getTensor[Double].toArray))
    }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: ElementwiseProduct): ElementwiseProduct = {
    new ElementwiseProduct(uid = uid)
  }

  override def sparkInputs(obj: ElementwiseProduct): Seq[ParamSpec] = {
    Seq("input" -> obj.inputCol)
  }

  override def sparkOutputs(obj: ElementwiseProduct): Seq[SimpleParamSpec] = {
    Seq("output" -> obj.outputCol  )
  }
}
