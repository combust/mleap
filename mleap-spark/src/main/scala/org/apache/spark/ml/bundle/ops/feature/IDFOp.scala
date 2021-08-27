package org.apache.spark.ml.bundle.ops.feature

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.OpModel
import org.apache.spark.ml.bundle.{ParamSpec, SimpleParamSpec, SimpleSparkOp, SparkBundleContext}
import org.apache.spark.ml.feature.IDFModel
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
      model.
        withValue("idf", Value.vector(obj.idf.toArray)).
        withValue("docFreq", Value.longList(obj.docFreq)).
        withValue("numDocs", Value.long(obj.numDocs))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): IDFModel = {
      val idf = Vectors.dense(model.value("idf").getTensor[Double].toArray)
      val docFreq = model.getValue("docFreq").
        map(_.getLongList.toArray).getOrElse(new Array[Long](idf.size))
      val numDocs = model.getValue("numDocs").map(_.getLong).getOrElse(-1L)
      val idfModel = new feature.IDFModel(
        idf = idf,
        docFreq = docFreq,
        numDocs = numDocs
      )
      new IDFModel(uid = "", idfModel = idfModel)
    }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: IDFModel): IDFModel = {
    val idfModel: feature.IDFModel = new feature.IDFModel(
      idf = Vectors.fromML(model.idf),
      docFreq = model.docFreq,
      numDocs = model.numDocs
    )
    new IDFModel(uid = uid, idfModel = idfModel)
  }

  override def sparkInputs(obj: IDFModel): Seq[ParamSpec] = {
    Seq("input" -> obj.inputCol)
  }

  override def sparkOutputs(obj: IDFModel): Seq[SimpleParamSpec] = {
    Seq("output" -> obj.outputCol)
  }
}
