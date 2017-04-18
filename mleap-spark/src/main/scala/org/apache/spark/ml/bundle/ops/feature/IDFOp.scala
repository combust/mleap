package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.feature.IDFModel
import org.apache.spark.mllib.feature
import org.apache.spark.mllib.linalg.Vectors

/**
  * Created by hollinwilkins on 12/28/16.
  */
class IDFOp extends OpNode[SparkBundleContext, IDFModel, IDFModel] {
  override val Model: OpModel[SparkBundleContext, IDFModel] = new OpModel[SparkBundleContext, IDFModel] {
    override val klazz: Class[IDFModel] = classOf[IDFModel]

    override def opName: String = Bundle.BuiltinOps.feature.idf

    override def store(model: Model, obj: IDFModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      model.withAttr("idf", Value.vector(obj.idf.toArray))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): IDFModel = {
      val idfModel = new feature.IDFModel(Vectors.dense(model.value("idf").getTensor[Double].toArray))
      new IDFModel(uid = "", idfModel = idfModel)
    }
  }

  override val klazz: Class[IDFModel] = classOf[IDFModel]

  override def name(node: IDFModel): String = node.uid

  override def model(node: IDFModel): IDFModel = node

  override def load(node: Node, model: IDFModel)
                   (implicit context: BundleContext[SparkBundleContext]): IDFModel = {
    new IDFModel(uid = node.name,
      idfModel = new feature.IDFModel(Vectors.dense(model.idf.toArray))).
      setInputCol(node.shape.standardInput.name).
      setOutputCol(node.shape.standardOutput.name)
  }

  override def shape(node: IDFModel)(implicit context: BundleContext[SparkBundleContext]): Shape = {
    Shape().withStandardIO(node.getInputCol, node.getOutputCol)
  }
}
