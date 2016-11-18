package ml.combust.mleap.bundle.ops.clustering

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.core.clustering.GaussianMixtureModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.clustering.GaussianMixture
import org.apache.spark.ml.linalg.{Matrices, Vectors}
import org.apache.spark.ml.stat.distribution.MultivariateGaussian

/**
  * Created by hollinwilkins on 9/30/16.
  */
class GaussianMixtureOp extends OpNode[MleapContext, GaussianMixture, GaussianMixtureModel] {
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
      GaussianMixtureModel(gaussians, weights)
    }
  }

  override val klazz: Class[GaussianMixture] = classOf[GaussianMixture]

  override def name(node: GaussianMixture): String = node.uid

  override def model(node: GaussianMixture): GaussianMixtureModel = node.model

  override def load(node: Node, model: GaussianMixtureModel)
                   (implicit context: BundleContext[MleapContext]): GaussianMixture = {
    GaussianMixture(uid = node.name,
      featuresCol = node.shape.input("features").name,
      predictionCol = node.shape.output("prediction").name,
      probabilityCol = node.shape.getOutput("probability").map(_.name),
      model = model)
  }

  override def shape(node: GaussianMixture): Shape = Shape().withInput(node.featuresCol, "features").
    withOutput(node.predictionCol, "prediction").
    withOutput(node.probabilityCol, "probability")
}
