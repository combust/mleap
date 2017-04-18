package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.runtime.transformer.feature.BucketizerUtil._
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.feature.Bucketizer
import org.apache.spark.sql.mleap.TypeConverters.fieldType

/**
  * Created by mikhail on 9/22/16.
  */
class BucketizerOp extends OpNode[SparkBundleContext, Bucketizer, Bucketizer] {
  override val Model: OpModel[SparkBundleContext, Bucketizer] = new OpModel[SparkBundleContext, Bucketizer] {
    override val klazz: Class[Bucketizer] = classOf[Bucketizer]

    override def opName: String = Bundle.BuiltinOps.feature.bucketizer

    override def store(model: Model, obj: Bucketizer)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      model.withAttr("splits", Value.doubleList(obj.getSplits))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): Bucketizer = {
      new Bucketizer(uid = "").setSplits(restoreSplits(model.value("splits").getDoubleList.toArray))
    }
  }

  override val klazz: Class[Bucketizer] = classOf[Bucketizer]

  override def name(node: Bucketizer): String = node.uid

  override def model(node: Bucketizer): Bucketizer = node

  override def load(node: Node, model: Bucketizer)
                   (implicit context: BundleContext[SparkBundleContext]): Bucketizer = {
    new Bucketizer(uid = node.name).
      setInputCol(node.shape.standardInput.name).
      setOutputCol(node.shape.standardOutput.name).
      setSplits(model.getSplits)
  }

  override def shape(node: Bucketizer)(implicit context: BundleContext[SparkBundleContext]): Shape = {
    val dataset = context.context.dataset
    Shape().withStandardIO(node.getInputCol, fieldType(node.getInputCol, dataset),
      node.getOutputCol, fieldType(node.getOutputCol, dataset))
  }
}
