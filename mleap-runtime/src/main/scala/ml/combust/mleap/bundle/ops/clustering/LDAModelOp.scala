package ml.combust.mleap.bundle.ops.clustering

import breeze.linalg.{Matrix, DenseMatrix => BDM, DenseVector => BDV}
import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.core.clustering.LocalLDAModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.clustering.LDAModel
import ml.combust.mleap.tensor.DenseTensor

/**
  * Created by mageswarand on 3/3/17.
  */
class LDAModelOp extends OpNode[MleapContext, LDAModel, LocalLDAModel] {

  override val Model: OpModel[MleapContext, LocalLDAModel] = new OpModel[MleapContext, LocalLDAModel] {
    override val klazz: Class[LocalLDAModel] = classOf[LocalLDAModel]

    override def opName: String = Bundle.BuiltinOps.clustering.lda

    override def store(model: Model, obj: LocalLDAModel)(implicit context: BundleContext[MleapContext]): Model = {
      val topicMatrixArray = obj.topicsMatrix.toDenseMatrix.toArray //TODO should we add SparseMatrix?
      val topicMatrixRows = obj.topicsMatrix.rows
      val topicMatrixCols = obj.topicsMatrix.cols

      model.withValue("vocabSize", Value.int(obj.vocabSize)).
        withValue("docConcentration", Value.doubleList(obj.docConcentration.toArray)).
        withValue("topicConcentration", Value.double(obj.topicConcentration)).
        withValue("topicMatrix", Value.tensor[Double](DenseTensor(topicMatrixArray, Seq(topicMatrixRows, topicMatrixCols))))
    }

    override def load(model: Model)(implicit context: BundleContext[MleapContext]): LocalLDAModel = {

      val topicMatrix = model.value("topicMatrix").getTensor[Double]
      val rows = topicMatrix.dimensions.head
      val cols = topicMatrix.dimensions(1)

      new LocalLDAModel(Matrix.create( rows, cols, topicMatrix.toArray),
        BDV(model.value("docConcentration").getDoubleList.toArray), //TODO should we add Sparse?
        model.value("topicConcentration").getDouble
      )
    }
  }

  override val klazz: Class[LDAModel] = classOf[LDAModel]

  override def name(node: LDAModel): String = node.uid

  override def model(node: LDAModel): LocalLDAModel = node.model

  override def shape(node: LDAModel): NodeShape = {
    NodeShape().
      withInput(node.featureCol, "features").
      withOutput(node.topicDistributionCol, "topicDistribution")
  }

  override def load(node: Node, model: LocalLDAModel)(implicit context: BundleContext[MleapContext]): LDAModel = {

    LDAModel(node.name,
      node.shape.input("features").name,
      node.shape.output("topicDistribution").name,
      model)
  }
}
