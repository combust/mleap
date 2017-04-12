package org.apache.spark.ml.bundle.ops.clustering

import ml.combust.mleap.tensor.DenseTensor
import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl.{Model, Node, Shape}
import ml.combust.bundle.op.{OpModel, OpNode}
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.clustering.LocalLDAModel
import org.apache.spark.mllib.clustering.{LocalLDAModel => oldLocalLDAModel}
import ml.combust.bundle.dsl._
import org.apache.spark.mllib.linalg.DenseMatrix
import org.apache.spark.sql.SparkSession


/**
  * Created by mageswarand on 15/2/17.
  */
class LDAModelOp extends OpNode[SparkBundleContext, LocalLDAModel, LocalLDAModel] {

  override val Model: OpModel[SparkBundleContext, LocalLDAModel] = new OpModel[SparkBundleContext, LocalLDAModel] {
    override val klazz: Class[LocalLDAModel] = classOf[LocalLDAModel]

    override def opName: String = Bundle.BuiltinOps.clustering.lda

    override def store(model: Model, obj: LocalLDAModel)(implicit context: BundleContext[SparkBundleContext]): Model = {
      val topicMatrixArray: Array[Double] = obj.topicsMatrix.asBreeze.toDenseMatrix.toArray
      val topicMatrixRows = obj.topicsMatrix.numRows
      val topicMatrixCols = obj.topicsMatrix.numCols

      model.withAttr("vocabSize", Value.int(obj.vocabSize)).
        withAttr("docConcentration", Value.doubleList(obj.getEffectiveDocConcentration)).
        withAttr("topicConcentration", Value.double(obj.getEffectiveTopicConcentration)).
        withAttr("topicMatrix", Value.tensor[Double](DenseTensor[Double](topicMatrixArray, Seq(topicMatrixRows, topicMatrixCols))))
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
  override val klazz: Class[LocalLDAModel] = classOf[LocalLDAModel]

  override def name(node: LocalLDAModel): String = node.uid

  override def model(node: LocalLDAModel): LocalLDAModel = node

  override def shape(node: LocalLDAModel): Shape = {
    Shape().
      withInput("features", "features").
      withOutput("topicDistribution", "topicDistribution")
  }

  override def load(node: Node, model: LocalLDAModel)(implicit context: BundleContext[SparkBundleContext]): LocalLDAModel = {
    model.setFeaturesCol(node.shape.input("features").name)
  }
}
