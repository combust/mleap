package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.feature.VectorSlicer
import org.apache.spark.sql.mleap.TypeConverters.fieldType

/**
  * Created by hollinwilkins on 12/28/16.
  */
class VectorSlicerOp extends OpNode[SparkBundleContext, VectorSlicer, VectorSlicer] {
  override val Model: OpModel[SparkBundleContext, VectorSlicer] = new OpModel[SparkBundleContext, VectorSlicer] {
    override val klazz: Class[VectorSlicer] = classOf[VectorSlicer]

    override def opName: String = Bundle.BuiltinOps.feature.vector_slicer

    override def store(model: Model, obj: VectorSlicer)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      model.withAttr("indices", Value.longList(obj.getIndices.map(_.toLong).toSeq))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): VectorSlicer = {
      new VectorSlicer(uid = "").setIndices(model.value("indices").getLongList.map(_.toInt).toArray)
    }
  }

  override val klazz: Class[VectorSlicer] = classOf[VectorSlicer]

  override def name(node: VectorSlicer): String = node.uid

  override def model(node: VectorSlicer): VectorSlicer = node

  override def load(node: Node, model: VectorSlicer)
                   (implicit context: BundleContext[SparkBundleContext]): VectorSlicer = {
    new VectorSlicer(uid = node.name).setIndices(model.getIndices).
      setInputCol(node.shape.standardInput.name).
      setOutputCol(node.shape.standardOutput.name)
  }

  override def shape(node: VectorSlicer)(implicit context: BundleContext[SparkBundleContext]): Shape = {
    val dataset = context.context.dataset
    Shape().withStandardIO(node.getInputCol, fieldType(node.getInputCol, dataset),
      node.getOutputCol, fieldType(node.getOutputCol, dataset))
  }
}
