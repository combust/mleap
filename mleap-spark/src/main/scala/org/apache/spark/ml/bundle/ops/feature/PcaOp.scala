package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.feature.PCAModel
import org.apache.spark.ml.linalg.{DenseMatrix, DenseVector}

/**
  * Created by hollinwilkins on 10/12/16.
  */
class PcaOp extends OpNode[SparkBundleContext, PCAModel, PCAModel] {
  override val Model: OpModel[SparkBundleContext, PCAModel] = new OpModel[SparkBundleContext, PCAModel] {
    override val klazz: Class[PCAModel] = classOf[PCAModel]

    override def opName: String = Bundle.BuiltinOps.feature.pca

    override def store(context: BundleContext[SparkBundleContext], model: Model, obj: PCAModel): Model = {
      model.withAttr("principal_components", Value.tensor[Double](obj.pc.values.toSeq,
        Seq(obj.pc.numRows, obj.pc.numCols)))
    }

    override def load(context: BundleContext[SparkBundleContext], model: Model): PCAModel = {
      val tt = model.value("principal_components").bundleDataType.getTensor
      val values = model.value("principal_components").getTensor[Double].toArray
      new PCAModel(uid = "",
        pc = new DenseMatrix(tt.dimensions.head, tt.dimensions(1), values),
        explainedVariance = new DenseVector(Array()))
    }
  }

  override val klazz: Class[PCAModel] = classOf[PCAModel]

  override def name(node: PCAModel): String = node.uid

  override def model(node: PCAModel): PCAModel = node

  override def load(context: BundleContext[SparkBundleContext], node: Node, model: PCAModel): PCAModel = {
    new PCAModel(uid = node.name,
      pc = model.pc,
      explainedVariance = model.explainedVariance)
  }

  override def shape(node: PCAModel): Shape = Shape().withStandardIO(node.getInputCol, node.getOutputCol)
}
