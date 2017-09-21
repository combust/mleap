package org.apache.spark.ml.bundle.ops.clustering

import ml.combust.mleap.tensor.DenseTensor
import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl.Model
import ml.combust.bundle.op.{OpModel, OpNode}
import org.apache.spark.ml.bundle.{ParamSpec, SimpleParamSpec, SimpleSparkOp, SparkBundleContext}
import org.apache.spark.ml.clustering.LocalLDAModel
import org.apache.spark.mllib.clustering.{LocalLDAModel => oldLocalLDAModel}
import ml.combust.bundle.dsl._
import org.apache.spark.mllib.linalg.DenseMatrix
import org.apache.spark.sql.SparkSession


/**
  * Created by mageswarand on 15/2/17.
  */
class LDAModelOp extends SimpleSparkOp[LocalLDAModel] {

  override val Model: OpModel[SparkBundleContext, LocalLDAModel] = new OpModel[SparkBundleContext, LocalLDAModel] {
    override val klazz: Class[LocalLDAModel] = classOf[LocalLDAModel]

    override def opName: String = Bundle.BuiltinOps.clustering.lda

    override def store(model: Model, obj: LocalLDAModel)(implicit context: BundleContext[SparkBundleContext]): Model = {
      val topicMatrixArray: Array[Double] = obj.topicsMatrix.asBreeze.toDenseMatrix.toArray
      val topicMatrixRows = obj.topicsMatrix.numRows
      val topicMatrixCols = obj.topicsMatrix.numCols

      model.withValue("vocabSize", Value.int(obj.vocabSize)).
        withValue("docConcentration", Value.doubleList(obj.getEffectiveDocConcentration)).
        withValue("topicConcentration", Value.double(obj.getEffectiveTopicConcentration)).
        withValue("topicMatrix", Value.tensor[Double](DenseTensor[Double](topicMatrixArray, Seq(topicMatrixRows, topicMatrixCols))))
    }

    override def load(model: Model)(implicit context: BundleContext[SparkBundleContext]): LocalLDAModel  = {
      val vocabSize = model.value("vocabSize").getInt
      val topicConcentration = model.value("topicConcentration").getDouble
      val docConcentration = model.value("docConcentration").getDoubleList.toArray
      val topicMatrix = model.value("topicMatrix").getTensor[Double]
      val rows = topicMatrix.dimensions.head
      val cols = topicMatrix.dimensions(1)

      new LocalLDAModel("",
        vocabSize,
        new oldLocalLDAModel(new DenseMatrix(rows,cols, topicMatrix.toArray, false),
          new org.apache.spark.mllib.linalg.DenseVector(docConcentration), topicConcentration),
        SparkSession.builder().getOrCreate())
    }
  }

  override def sparkLoad(uid: String, shape: NodeShape, model: LocalLDAModel): LocalLDAModel = {
    val field = classOf[LocalLDAModel].getDeclaredField("oldLocalModel")
    field.setAccessible(true)
    val oldLocalModel = field.get(model).asInstanceOf[oldLocalLDAModel]

    new LocalLDAModel(uid = uid,
      vocabSize = model.vocabSize,
      oldLocalModel = oldLocalModel,
      sparkSession = model.sparkSession)
  }

  override def sparkInputs(obj: LocalLDAModel): Seq[ParamSpec] = {
    Seq("features" -> obj.featuresCol)
  }

  override def sparkOutputs(obj: LocalLDAModel): Seq[SimpleParamSpec] = {
    Seq("prediction" -> obj.topicDistributionCol)
  }
}
