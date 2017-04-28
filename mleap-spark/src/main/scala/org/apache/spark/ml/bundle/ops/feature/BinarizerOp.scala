package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.feature.Binarizer
import org.apache.spark.sql.mleap.TypeConverters.mleapType
import ml.combust.mleap.runtime.types.BundleTypeConverters._

/**
  * Created by fshabbir on 12/1/16.
  */
class BinarizerOp extends OpNode[SparkBundleContext, Binarizer, Binarizer] {
  override val Model: OpModel[SparkBundleContext, Binarizer] = new OpModel[SparkBundleContext, Binarizer] {
    override val klazz: Class[Binarizer] = classOf[Binarizer]

    override def opName: String = Bundle.BuiltinOps.feature.binarizer

    override def store(model: Model, obj: Binarizer)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      context.context.dataset.map(dataset => {
        model.withAttr("input_types", Value.dataType(mleapType(dataset.schema(obj.getInputCol).dataType)))
             .withAttr("output_types", Value.dataType(mleapType(dataset.schema(obj.getOutputCol).dataType)))
      }).getOrElse(model).withAttr("threshold", Value.double(obj.getThreshold))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): Binarizer = {
      new Binarizer(uid = "").setThreshold(model.value("threshold").getDouble)
    }
  }

  override val klazz: Class[Binarizer] = classOf[Binarizer]

  override def name(node: Binarizer): String = node.uid

  override def model(node: Binarizer): Binarizer = node

  override def load(node: Node, model: Binarizer)
                   (implicit context: BundleContext[SparkBundleContext]): Binarizer = {
    new Binarizer(uid = node.name).
      setInputCol(node.shape.standardInput.name).
      setOutputCol(node.shape.standardOutput.name).
      setThreshold(model.getThreshold)
  }

  override def shape(node: Binarizer): Shape = Shape().withStandardIO(node.getInputCol, node.getOutputCol)

}
