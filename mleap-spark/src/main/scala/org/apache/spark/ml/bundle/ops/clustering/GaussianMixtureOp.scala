package org.apache.spark.ml.bundle.ops.clustering

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.tensor.{DenseTensor, Tensor}
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.clustering.GaussianMixtureModel
import org.apache.spark.ml.linalg.{Matrices, Vectors}
import org.apache.spark.ml.stat.distribution.MultivariateGaussian

/**
  * Created by hollinwilkins on 9/30/16.
  */
class GaussianMixtureOp extends OpNode[SparkBundleContext, GaussianMixtureModel, GaussianMixtureModel] {
  override val Model: OpModel[SparkBundleContext, GaussianMixtureModel] = new OpModel[SparkBundleContext, GaussianMixtureModel] {
    override val klazz: Class[GaussianMixtureModel] = classOf[GaussianMixtureModel]

    override def opName: String = Bundle.BuiltinOps.clustering.gaussian_mixture

    override def store(model: Model, obj: GaussianMixtureModel)
                      (implicit context: BundleContext[SparkBundleContext]): Model = {
      val (rows, cols) = obj.gaussians.headOption.
        map(g => (g.cov.numRows, g.cov.numCols)).
        getOrElse((-1, -1))
      val (means, covs) = obj.gaussians.map(g => (g.mean, g.cov)).unzip

      model.withAttr("means", Value.tensorList(means.map(_.toArray).map(Tensor.denseVector))).
        withAttr("covs", Value.tensorList(covs.map(m => DenseTensor(m.toArray, Seq(m.numRows, m.numCols))))).
        withAttr("weights", Value.doubleList(obj.weights.toSeq))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[SparkBundleContext]): GaussianMixtureModel = {
      val means = model.value("means").getTensorList[Double].map(values => Vectors.dense(values.toArray))
      val covs = model.value("covs").getTensorList[Double].map(values => Matrices.dense(values.dimensions.head, values.dimensions(1), values.toArray))
      val gaussians = means.zip(covs).map {
        case (mean, cov) => new MultivariateGaussian(mean, cov)
      }.toArray
      val weights = model.value("weights").getDoubleList.toArray

      new GaussianMixtureModel(uid = "",
        gaussians = gaussians,
        weights = weights)
    }
  }

  override val klazz: Class[GaussianMixtureModel] = classOf[GaussianMixtureModel]

  override def name(node: GaussianMixtureModel): String = node.uid

  override def model(node: GaussianMixtureModel): GaussianMixtureModel = node

  override def load(node: Node, model: GaussianMixtureModel)
                   (implicit context: BundleContext[SparkBundleContext]): GaussianMixtureModel = {
    val gmm = new GaussianMixtureModel(uid = node.name,
      gaussians = model.gaussians,
      weights = model.weights)
    gmm.set(gmm.featuresCol, node.shape.input("features").name)
    gmm.set(gmm.predictionCol, node.shape.output("prediction").name)
    for(p <- node.shape.getOutput("probability")) {
      gmm.set(gmm.probabilityCol, p.name)
    }
    gmm
  }

  override def shape(node: GaussianMixtureModel): NodeShape = {
    val probability = if(node.isDefined(node.probabilityCol)) Some(node.getProbabilityCol) else None

    NodeShape().withInput(node.getFeaturesCol, "features").
      withOutput(node.getPredictionCol, "prediction").
      withOutput(probability, "probability")
  }
}
