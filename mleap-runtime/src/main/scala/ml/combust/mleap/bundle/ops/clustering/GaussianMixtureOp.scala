package ml.combust.mleap.bundle.ops.clustering

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl._
import ml.combust.bundle.op.{OpModel, OpNode}
import ml.combust.mleap.core.clustering.GaussianMixtureModel
import ml.combust.mleap.runtime.MleapContext
import ml.combust.mleap.runtime.transformer.clustering.GaussianMixture
import ml.combust.mleap.tensor.{DenseTensor, Tensor}
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
      val (means, covs) = obj.gaussians.map(g => (g.mean, g.cov)).unzip
      model.withValue("means", Value.tensorList(means.map(m => Tensor.denseVector(m.toArray)))).
        withValue("covs", Value.tensorList(covs.map(c => DenseTensor(c.toArray, Seq(c.numRows, c.numCols))))).
        withValue("weights", Value.doubleList(obj.weights.toSeq))
    }

    override def load(model: Model)
                     (implicit context: BundleContext[MleapContext]): GaussianMixtureModel = {
      val means = model.value("means").getTensorList[Double].map(values => Vectors.dense(values.toArray))
      val covs = model.value("covs").getTensorList[Double].map {
        values => Matrices.dense(values.dimensions.head, values.dimensions(1), values.toArray)
      }
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

  override def shape(node: GaussianMixture): NodeShape = NodeShape().withInput(node.featuresCol, "features").
    withOutput(node.predictionCol, "prediction").
    withOutput(node.probabilityCol, "probability")
}
