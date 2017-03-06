package ml.combust.mleap.bundle.ops.clustering

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.tensor.DenseTensor

/**
  * Created by mageswarand on 3/3/17.
  *
  * https://github.com/combust/mleap/blob/master/mleap-runtime/src/main/scala/ml/combust/mleap/bundle/ops/classification/LogisticRegressionOp.scala
  *
  */
class LDAModelOp extends OpNode[MleapContext, LDAModel, LocalLDAModel] {

  override val Model: OpModel[MleapContext, LocalLDAModel] = new OpModel[MleapContext, LocalLDAModel] {
    override val klazz: Class[LocalLDAModel] = classOf[LocalLDAModel]

    override def opName: String = "lda_local_model_op"

    override def store(model: Model, obj: LocalLDAModel)(implicit context: BundleContext[MleapContext]): Model = {
      val topicMatrixArray = obj.topicsMatrix.toDenseMatrix.toArray
      val topicMatrixRows = obj.topicsMatrix.rows
      val topicMatrixCols = obj.topicsMatrix.cols

      println("Modle Paramaters during MLEAP saving...")
      println("Rows: " + topicMatrixRows)
      println("Cols: " + topicMatrixCols)
      println("Array: " + topicMatrixArray.size)

      model.withAttr("vocabSize", Value.int(obj.vocabSize)).
        withAttr("docConcentration", Value.doubleList(obj.docConcentration.toArray)).
        withAttr("topicConcentration", Value.double(obj.topicConcentration)).
        withAttr("topicMatrix", Value.tensor[Double](DenseTensor(topicMatrixArray, Seq(topicMatrixRows, topicMatrixCols))))
    }

    override def load(model: Model)(implicit context: BundleContext[MleapContext]): LocalLDAModel = {
      println("Modle Paramaters during MLEAP reading...")


      val topicMatrix = model.value("topicMatrix").getTensor[Double]
      val rows = topicMatrix.dimensions.head
      val cols = topicMatrix.dimensions(1)

      println("Rows: " + rows)
      println("Cols: " + cols)
      println("Array: " + topicMatrix.toArray.size)

      new LocalLDAModel(Matrix.create( rows, cols, topicMatrix.toArray),
        BDV(model.value("docConcentration").getDoubleList.toArray),
        model.value("topicConcentration").getDouble
      )
    }
  }

  override val klazz: Class[LDAModel] = classOf[LDAModel]

  override def name(node: LDAModel): String = node.uid

  override def model(node: LDAModel): LocalLDAModel = node.model

  override def shape(node: LDAModel): Shape = {
    println("LDAModelCT shape"  + " " +
      node.featureCol + " " +
      node.topicDistributionCol)

    Shape().
      withInput(node.featureCol, "features").
      withOutput(node.topicDistributionCol, "topicDistribution")
  }

  override def load(node: Node, model: LocalLDAModel)(implicit context: BundleContext[MleapContext]): LDAModel = {
    try {println("Load LDAModel :" +  " " +
      node.shape.input("features").name +" " +
      node.shape.output("topicDistribution").name)}
    catch {
      case e => println("$$$$ " + e)
    }

    LDAModel(node.name,
      node.shape.input("features").name,
      node.shape.output("topicDistribution").name,
      model)
  }
}
