package org.apache.spark.ml.bundle.ops.classification

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
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
      model.withAttr("num_features", Value.long(obj.numFeatures)).
        withAttr("num_classes", Value.long(obj.numClasses)).
        withAttr("pi", Value.doubleVector(obj.pi.toArray)).
        withAttr("theta", Value.tensor(obj.theta.toArray.toSeq, Seq(obj.theta.numRows, obj.theta.numCols))).
        withAttr("model_type", Value.string(obj.getModelType))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): NaiveBayesModel = {
      val Seq(rows, cols) = model.value("theta").bundleDataType.getTensor.dimensions
      val nb = new NaiveBayesModel(uid = "",
        pi = Vectors.dense(model.value("pi").getDoubleVector.toArray),
        theta = Matrices.dense(rows, cols, model.value("theta").getTensor[Double].toArray))
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

  override def shape(node: NaiveBayesModel): Shape = {
    val rawPrediction = if(node.isDefined(node.rawPredictionCol)) Some(node.getRawPredictionCol) else None
    val probability = if(node.isDefined(node.probabilityCol)) Some(node.getProbabilityCol) else None

    Shape().withInput(node.getFeaturesCol, "features").
      withOutput(node.getPredictionCol, "prediction").
      withOutput(rawPrediction, "raw_prediction").
      withOutput(probability, "probability")
  }
}
