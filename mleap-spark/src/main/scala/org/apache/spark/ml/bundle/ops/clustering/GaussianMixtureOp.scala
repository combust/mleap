package org.apache.spark.ml.bundle.ops.clustering

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.runtime.MleapContext
import org.apache.spark.ml.clustering.GaussianMixtureModel
import org.apache.spark.ml.linalg.{Matrices, Vectors}
import org.apache.spark.ml.stat.distribution.MultivariateGaussian

/**
  * Created by hollinwilkins on 9/30/16.
  */
class GaussianMixtureOp extends OpNode[MleapContext, GaussianMixtureModel, GaussianMixtureModel] {
  override val Model: OpModel[MleapContext, GaussianMixtureModel] = new OpModel[MleapContext, GaussianMixtureModel] {
    override val klazz: Class[GaussianMixtureModel] = classOf[GaussianMixtureModel]

    override def opName: String = Bundle.BuiltinOps.clustering.gaussian_mixture

    override def store(model: Model, obj: GaussianMixtureModel)
                      (implicit context: BundleContext[MleapContext]): Model = {
      val (rows, cols) = obj.gaussians.headOption.
        map(g => (g.cov.numRows, g.cov.numCols)).
        getOrElse((-1, -1))
      val (means, covs) = obj.gaussians.map(g => (g.mean, g.cov)).unzip

      model.withAttr("means", Value.tensorList(means.map(_.toArray.toSeq).toSeq, Seq(-1))).
        withAttr("covs", Value.tensorList(covs.map(_.toArray.toSeq).toSeq, Seq(rows, cols))).
        withAttr("weights", Value.doubleList(obj.weights.toSeq))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): GaussianMixtureModel = {
      val Seq(rows, cols) = model.value("covs").bundleDataType.getList.getBase.getTensor.dimensions
      val means = model.value("means").getTensorList[Double].map(values => Vectors.dense(values.toArray))
      val covs = model.value("covs").getTensorList[Double].map(values => Matrices.dense(rows, cols, values.toArray))
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
                   (implicit context: BundleContext[MleapContext]): GaussianMixtureModel = {
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

  override def shape(node: GaussianMixtureModel): Shape = {
    val probability = if(node.isSet(node.probabilityCol)) Some(node.getProbabilityCol) else None

    Shape().withInput(node.getFeaturesCol, "features").
      withOutput(node.getPredictionCol, "prediction").
      withOutput(probability, "probability")
  }
}
