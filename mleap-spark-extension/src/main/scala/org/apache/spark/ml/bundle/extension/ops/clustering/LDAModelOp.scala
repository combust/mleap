package org.apache.spark.ml.bundle.extension.ops.clustering

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl.{Model, Node, Shape, _}
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.tensor.DenseTensor
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.mllib.clustering.{LocalLDAModel => oldLocalLDAModel}


/**
  * Created by mageswarand on 15/2/17.
  */
class LDAModelOp extends OpNode[SparkBundleContext, LocalLDAModel, LocalLDAModel] {

  override val Model: OpModel[SparkBundleContext, LocalLDAModel] = new OpModel[SparkBundleContext, LocalLDAModel] {
    override val klazz: Class[LocalLDAModel] = classOf[LocalLDAModel]

    override def opName: String = "lda_local_model_op"

    override def store(model: Model, obj: LocalLDAModel)(implicit context: BundleContext[SparkBundleContext]): Model = {
      val topicMatrixArray = obj.topicsMatrix.asBreeze.toDenseMatrix.toArray
      val topicMatrixRows = obj.topicsMatrix.numRows
      val topicMatrixCols = obj.topicsMatrix.numCols


      println("Modle Paramaters during Spark saving...")
      println("Rows: " + topicMatrixRows)
      println("Cols: " + topicMatrixCols)
      println("Array: " + topicMatrixArray.size)

      model.withAttr("vocabSize", Value.int(obj.vocabSize)).
        withAttr("docConcentration", Value.doubleList(obj.getEffectiveDocConcentration.toList)).
        withAttr("topicConcentration", Value.double(obj.getEffectiveTopicConcentration)).
        withAttr("topicMatrix", Value.tensor[Double](DenseTensor(topicMatrixArray, Seq(topicMatrixRows, topicMatrixCols))))
    }

    override def load(model: Model)(implicit context: BundleContext[SparkBundleContext]): LocalLDAModel  = {
      //      val vocabSize = model.value("vocabSize").getInt
      val topicConcentration = model.value("topicConcentration")
      val docConcentration = model.value("docConcentration")
      new LocalLDAModel("",
        5000, //TODO dummy
        new oldLocalLDAModel(new DenseMatrix(0,0, Array(), false), new org.apache.spark.mllib.linalg.DenseVector(Array()), 0.5),
        SparkSession.builder().getOrCreate())
    }
  }
  override val klazz: Class[LocalLDAModel] = classOf[LocalLDAModel]

  override def name(node: LocalLDAModel): String = node.uid

  override def model(node: LocalLDAModel): LocalLDAModel = node

  override def shape(node: LocalLDAModel): Shape = {
    Shape().
      withInput("features", "features").
      withOutput("topicDistribution", "topicDistribution")
  }

  override def load(node: Node, model: LocalLDAModel)(implicit context: BundleContext[SparkBundleContext]): LocalLDAModel = {
    new LocalLDAModel("",
      5000, //TODO dummy
      new oldLocalLDAModel(new DenseMatrix(0,0, Array(), false), new org.apache.spark.mllib.linalg.DenseVector(Array()), 0.5),
      SparkSession.builder().getOrCreate())
  }
}
