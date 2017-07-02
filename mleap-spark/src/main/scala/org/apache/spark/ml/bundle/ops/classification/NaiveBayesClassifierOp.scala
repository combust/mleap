package org.apache.spark.ml.bundle.ops.classification

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.tensor.DenseTensor
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.classification.NaiveBayesModel
import org.apache.spark.ml.linalg.{Matrices, Vectors}

/**
  * Created by fshabbir on 12/25/16.
  */
class NaiveBayesClassifierOp extends OpNode[SparkBundleContext, NaiveBayesModel, NaiveBayesModel] {
  override val Model: OpModel[SparkBundleContext, NaiveBayesModel] = new OpModel[SparkBundleContext, NaiveBayesModel] {
    override val klazz: Class[NaiveBayesModel] = classOf[NaiveBayesModel]

    override def opName: String = Bundle.BuiltinOps.classification.naive_bayes

    override def store(model: Model, obj: NaiveBayesModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      model.withValue("num_features", Value.long(obj.numFeatures)).
        withValue("num_classes", Value.long(obj.numClasses)).
        withValue("pi", Value.vector(obj.pi.toArray)).
        withValue("theta", Value.tensor(DenseTensor(obj.theta.toArray, Seq(obj.theta.numRows, obj.theta.numCols)))).
        withValue("model_type", Value.string(obj.getModelType))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): NaiveBayesModel = {
      val theta = model.value("theta").getTensor[Double]
      val nb = new NaiveBayesModel(uid = "",
        pi = Vectors.dense(model.value("pi").getTensor[Double].toArray),
        theta = Matrices.dense(theta.dimensions.head, theta.dimensions(1), theta.toArray))
      val modelType = model.value("model_type").getString
      nb.set(nb.modelType, modelType)
    }

  }
  override val klazz: Class[NaiveBayesModel] = classOf[NaiveBayesModel]

  override def name(node: NaiveBayesModel): String = node.uid

  override def model(node: NaiveBayesModel): NaiveBayesModel = node

  override def load(node: Node, model: NaiveBayesModel)(implicit context: BundleContext[SparkBundleContext]): NaiveBayesModel = {
    var nb = new NaiveBayesModel(uid = node.name,
      pi = model.pi,
      theta = model.theta).
      setFeaturesCol(node.shape.input("features").name).
      setPredictionCol(node.shape.output("prediction").name)
    nb = node.shape.getOutput("probability").map(p => nb.setProbabilityCol(p.name)).getOrElse(nb)
    node.shape.getOutput("raw_prediction").map(rp => nb.setRawPredictionCol(rp.name)).getOrElse(nb)
  }

  override def shape(node: NaiveBayesModel): NodeShape = {
    val rawPrediction = if(node.isDefined(node.rawPredictionCol)) Some(node.getRawPredictionCol) else None
    val probability = if(node.isDefined(node.probabilityCol)) Some(node.getProbabilityCol) else None

    NodeShape().withInput(node.getFeaturesCol, "features").
      withOutput(node.getPredictionCol, "prediction").
      withOutput(rawPrediction, "raw_prediction").
      withOutput(probability, "probability")
  }
}
