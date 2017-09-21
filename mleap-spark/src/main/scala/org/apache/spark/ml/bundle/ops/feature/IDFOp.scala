package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import org.apache.spark.ml.bundle.{ParamSpec, SimpleParamSpec, SimpleSparkOp, SparkBundleContext}
import org.apache.spark.ml.feature.IDFModel
import org.apache.spark.ml.param.Param
import org.apache.spark.mllib.feature
import org.apache.spark.mllib.linalg.Vectors

/**
  * Created by hollinwilkins on 12/28/16.
  */
class IDFOp extends SimpleSparkOp[IDFModel] {
  override val Model: OpModel[SparkBundleContext, IDFModel] = new OpModel[SparkBundleContext, IDFModel] {
    override val klazz: Class[IDFModel] = classOf[IDFModel]

    override def opName: String = Bundle.BuiltinOps.feature.idf

    override def store(model: Model, obj: IDFModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      model.withValue("idf", Value.vector(obj.idf.toArray))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): IDFModel = {
      val idfModel = new feature.IDFModel(Vectors.dense(model.value("idf").getTensor[Double].toArray))
      new IDFModel(uid = "", idfModel = idfModel)
    }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: IDFModel): IDFModel = {
    new IDFModel(uid = uid, idfModel = new feature.IDFModel(Vectors.dense(model.idf.toArray)))
  }

  override def sparkInputs(obj: IDFModel): Seq[ParamSpec] = {
    Seq("input" -> obj.inputCol)
  }

  override def sparkOutputs(obj: IDFModel): Seq[SimpleParamSpec] = {
    Seq("output" -> obj.outputCol)
  }
}
