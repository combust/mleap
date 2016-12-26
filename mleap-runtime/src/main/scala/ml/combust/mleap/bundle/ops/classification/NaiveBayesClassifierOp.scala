package ml.combust.mleap.bundle.ops.classification

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl.{Model, Node, Shape}
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.bundle.tree.MleapNodeWrapper
import ml.combust.mleap.core.classification.NaiveBayesModel.{Multinomial, Bernoulli}
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.classification.NaiveBayesClassifier
import ml.combust.mleap.core.classification.{NaiveBayesModel}
import ml.combust.bundle.dsl._
import org.apache.spark.ml.linalg.Matrices


/**
  * Created by fshabbir on 12/25/16.
  */
class NaiveBayesClassifierOp extends OpNode[MleapContext, NaiveBayesClassifier, NaiveBayesModel]{
  implicit val nodeWrapper = MleapNodeWrapper
  override val Model: OpModel[MleapContext, NaiveBayesModel] = new OpModel[MleapContext, NaiveBayesModel]{
    override val klazz: Class[NaiveBayesModel] = classOf[NaiveBayesModel]

    override def opName: String = Bundle.BuiltinOps.classification.naive_bayes

    override def store(model: Model, obj: NaiveBayesModel)(implicit context: BundleContext[MleapContext]): Model = {
      model.withAttr("num_features", Value.double(obj.numFeatures)).
        withAttr("num_classes", Value.double(obj.numClasses)).
        withAttr("pi", Value.doubleVector(obj.pi.toArray)).
        withAttr("theta", Value.tensor(obj.theta.toArray.toSeq, Seq(obj.theta.numRows, obj.theta.numCols))).
        withAttr("model_type", Value.string(obj.modelType.toString))
    }

    def modelType(modelType: String) = modelType match{
      case "bernoulli" => Bernoulli
      case "multinomial" => Multinomial
    }

    override def load(model: Model)(implicit context: BundleContext[MleapContext]): NaiveBayesModel = {
      val Seq(rows, cols) = model.value("theta").bundleDataType.getList.getBase.getTensor.dimensions
      val modelType = modelType(model.value("model_type").getString)
      new NaiveBayesModel(numFeatures = model.value("num_features").getDouble,
        numClasses = model.value("num_classes").getDouble,
        pi = model.value("pi").getDoubleVector,  //how to fix
        theta = Matrices.dense(rows, cols, model.value("theta").getTensor[Double].toArray),
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
  override def shape(node: NaiveBayesClassifier): SShape = Shape().withInput(node.featuresCol, "features").
    withOutput(node.predictionCol, "prediction").
    withOutput(node.rawPredictionCol, "raw_prediction").
    withOutput(node.probabilityCol, "probability")
  }
}
