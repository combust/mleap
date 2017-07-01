package ml.combust.mleap.bundle.ops.classification

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl.{Model, Node, NodeShape}
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.classification.NaiveBayesClassifier
import ml.combust.mleap.core.classification.NaiveBayesModel
import ml.combust.bundle.dsl._
import ml.combust.mleap.tensor.DenseTensor
import org.apache.spark.ml.linalg.{Matrices, Vectors}


/**
  * Created by fshabbir on 12/25/16.
  */
class NaiveBayesClassifierOp extends OpNode[MleapContext, NaiveBayesClassifier, NaiveBayesModel]{
  override val Model: OpModel[MleapContext, NaiveBayesModel] = new OpModel[MleapContext, NaiveBayesModel]{
    override val klazz: Class[NaiveBayesModel] = classOf[NaiveBayesModel]

    override def opName: String = Bundle.BuiltinOps.classification.naive_bayes

    override def store(model: Model, obj: NaiveBayesModel)(implicit context: BundleContext[MleapContext]): Model = {
      model.withAttr("num_features", Value.long(obj.numFeatures)).
        withAttr("num_classes", Value.long(obj.numClasses)).
        withAttr("pi", Value.vector(obj.pi.toArray)).
        withAttr("theta", Value.tensor(DenseTensor(obj.theta.toArray, Seq(obj.theta.numRows, obj.theta.numCols)))).
        withAttr("model_type", Value.string(obj.modelType.toString))
    }

    override def load(model: Model)(implicit context: BundleContext[MleapContext]): NaiveBayesModel = {
      val theta = model.value("theta").getTensor[Double]
      val modelType = NaiveBayesModel.forName(model.value("model_type").getString)
      new NaiveBayesModel(numFeatures = model.value("num_features").getLong.toInt,
        numClasses = model.value("num_classes").getLong.toInt,
        pi = Vectors.dense(model.value("pi").getTensor[Double].toArray),
        theta = Matrices.dense(theta.dimensions.head, theta.dimensions(1), theta.toArray),
        modelType = modelType)
    }

  }
  override val klazz: Class[NaiveBayesClassifier] = classOf[NaiveBayesClassifier]

  override def name(node: NaiveBayesClassifier): String = node.uid

  override def model(node: NaiveBayesClassifier): NaiveBayesModel = node.model

  override def load(node: Node, model: NaiveBayesModel)(implicit context: BundleContext[MleapContext]): NaiveBayesClassifier = {
    NaiveBayesClassifier(uid = node.name,
      featuresCol = node.shape.input("features").name,
      predictionCol = node.shape.output("prediction").name,
      rawPredictionCol = node.shape.getOutput("raw_prediction").map(_.name),
      probabilityCol = node.shape.getOutput("probability").map(_.name),
      model = model)
  }
  override def shape(node: NaiveBayesClassifier): NodeShape = NodeShape().withInput(node.featuresCol, "features").
    withOutput(node.predictionCol, "prediction").
    withOutput(node.rawPredictionCol, "raw_prediction").
    withOutput(node.probabilityCol, "probability")
}
