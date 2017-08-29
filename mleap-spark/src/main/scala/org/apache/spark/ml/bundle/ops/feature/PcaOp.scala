package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.tensor.DenseTensor
import org.apache.spark.ml.bundle.{ParamSpec, SimpleParamSpec, SimpleSparkOp, SparkBundleContext}
import org.apache.spark.ml.feature.PCAModel
import org.apache.spark.ml.linalg.{DenseMatrix, DenseVector}

/**
  * Created by hollinwilkins on 10/12/16.
  */
class PcaOp extends SimpleSparkOp[PCAModel] {
  override val Model: OpModel[SparkBundleContext, PCAModel] = new OpModel[SparkBundleContext, PCAModel] {
    override val klazz: Class[PCAModel] = classOf[PCAModel]

    override def opName: String = Bundle.BuiltinOps.feature.pca

    override def store(model: Model, obj: PCAModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      model.withValue("principal_components", Value.tensor[Double](DenseTensor(obj.pc.values,
        Seq(obj.pc.numRows, obj.pc.numCols))))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): PCAModel = {
      val values = model.value("principal_components").getTensor[Double]
      new PCAModel(uid = "",
        pc = new DenseMatrix(values.dimensions.head, values.dimensions(1), values.toArray),
        explainedVariance = new DenseVector(Array()))
    }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: PCAModel): PCAModel = {
    new PCAModel(uid = uid, pc = model.pc, explainedVariance = model.explainedVariance)
  }

  override def sparkInputs(obj: PCAModel): Seq[ParamSpec] = {
    Seq("input" -> obj.inputCol)
  }

  override def sparkOutputs(obj: PCAModel): Seq[SimpleParamSpec] = {
    Seq("output" -> obj.outputCol)
  }
}
