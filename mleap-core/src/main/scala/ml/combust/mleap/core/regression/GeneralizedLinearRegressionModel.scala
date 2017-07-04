package ml.combust.mleap.core.regression

import breeze.stats.{distributions => dist}
import ml.combust.mleap.core.annotation.SparkCode
import ml.combust.mleap.core.regression.GeneralizedLinearRegressionModel.FamilyAndLink
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.linalg.mleap.BLAS

/**
  * Created by hollinwilkins on 12/28/16.
  */
@SparkCode(uri = "https://github.com/apache/spark/blob/v2.0.0/mllib/src/main/scala/org/apache/spark/ml/regression/GeneralizedLinearRegression.scala")
object GeneralizedLinearRegressionModel {
  val epsilon: Double = 1E-16

  lazy val supportedFamilyAndLinkPairs = Set(
    Gaussian -> Identity, Gaussian -> Log, Gaussian -> Inverse,
    Binomial -> Logit, Binomial -> Probit, Binomial -> CLogLog,
    Poisson -> Log, Poisson -> Identity, Poisson -> Sqrt,
    Gamma -> Inverse, Gamma -> Identity, Gamma -> Log
  )

  /**
    * Wrapper of family and link combination used in the model.
    */
  class FamilyAndLink(val family: Family, val link: Link) extends Serializable {

    /** Linear predictor based on given mu. */
    def predict(mu: Double): Double = link.link(family.project(mu))

    /** Fitted value based on linear predictor eta. */
    def fitted(eta: Double): Double = family.project(link.unlink(eta))
  }

  /**
    * A description of the error distribution to be used in the model.
    *
    * @param name the name of the family.
    */
  abstract class Family(val name: String) extends Serializable {

    /** The default link instance of this family. */
    val defaultLink: Link

    /** Initialize the starting value for mu. */
    def initialize(y: Double, weight: Double): Double

    /** The variance of the endogenous variable's mean, given the value mu. */
    def variance(mu: Double): Double

    /** Deviance of (y, mu) pair. */
    def deviance(y: Double, mu: Double, weight: Double): Double

    /** Trim the fitted value so that it will be in valid range. */
    def project(mu: Double): Double = mu
  }

  object Family {

    /**
      * Gets the [[Family]] object from its name.
      *
      * @param name family name: "gaussian", "binomial", "poisson" or "gamma".
      */
    def fromName(name: String): Family = {
      name match {
        case Gaussian.name => Gaussian
        case Binomial.name => Binomial
        case Poisson.name => Poisson
        case Gamma.name => Gamma
      }
    }
  }

  /**
    * Gaussian exponential family distribution.
    * The default link for the Gaussian family is the identity link.
    */
  object Gaussian extends Family("gaussian") {

    val defaultLink: Link = Identity

    override def initialize(y: Double, weight: Double): Double = y

    override def variance(mu: Double): Double = 1.0

    override def deviance(y: Double, mu: Double, weight: Double): Double = {
      weight * (y - mu) * (y - mu)
    }

    override def project(mu: Double): Double = {
      if (mu.isNegInfinity) {
        Double.MinValue
      } else if (mu.isPosInfinity) {
        Double.MaxValue
      } else {
        mu
      }
    }
  }

  /**
    * Binomial exponential family distribution.
    * The default link for the Binomial family is the logit link.
    */
  object Binomial extends Family("binomial") {

    val defaultLink: Link = Logit

    override def initialize(y: Double, weight: Double): Double = {
      val mu = (weight * y + 0.5) / (weight + 1.0)
      require(mu > 0.0 && mu < 1.0, "The response variable of Binomial family" +
        s"should be in range (0, 1), but got $mu")
      mu
    }

    override def variance(mu: Double): Double = mu * (1.0 - mu)

    override def deviance(y: Double, mu: Double, weight: Double): Double = {
      val my = 1.0 - y
      2.0 * weight * (y * math.log(math.max(y, 1.0) / mu) +
        my * math.log(math.max(my, 1.0) / (1.0 - mu)))
    }

    override def project(mu: Double): Double = {
      if (mu < epsilon) {
        epsilon
      } else if (mu > 1.0 - epsilon) {
        1.0 - epsilon
      } else {
        mu
      }
    }
  }

  /**
    * Poisson exponential family distribution.
    * The default link for the Poisson family is the log link.
    */
  object Poisson extends Family("poisson") {

    val defaultLink: Link = Log

    override def initialize(y: Double, weight: Double): Double = {
      require(y >= 0.0, "The response variable of Poisson family " +
        s"should be non-negative, but got $y")
      y
    }

    override def variance(mu: Double): Double = mu

    override def deviance(y: Double, mu: Double, weight: Double): Double = {
      2.0 * weight * (y * math.log(y / mu) - (y - mu))
    }

    override def project(mu: Double): Double = {
      if (mu < epsilon) {
        epsilon
      } else if (mu.isInfinity) {
        Double.MaxValue
      } else {
        mu
      }
    }
  }

  /**
    * Gamma exponential family distribution.
    * The default link for the Gamma family is the inverse link.
    */
  object Gamma extends Family("gamma") {

    val defaultLink: Link = Inverse

    override def initialize(y: Double, weight: Double): Double = {
      require(y > 0.0, "The response variable of Gamma family " +
        s"should be positive, but got $y")
      y
    }

    override def variance(mu: Double): Double = mu * mu

    override def deviance(y: Double, mu: Double, weight: Double): Double = {
      -2.0 * weight * (math.log(y / mu) - (y - mu)/mu)
    }

    override def project(mu: Double): Double = {
      if (mu < epsilon) {
        epsilon
      } else if (mu.isInfinity) {
        Double.MaxValue
      } else {
        mu
      }
    }
  }

  /**
    * A description of the link function to be used in the model.
    * The link function provides the relationship between the linear predictor
    * and the mean of the distribution function.
    *
    * @param name the name of link function.
    */
  abstract class Link(val name: String) extends Serializable {

    /** The link function. */
    def link(mu: Double): Double

    /** Derivative of the link function. */
    def deriv(mu: Double): Double

    /** The inverse link function. */
    def unlink(eta: Double): Double
  }

  object Link {

    /**
      * Gets the [[Link]] object from its name.
      *
      * @param name link name: "identity", "logit", "log",
      *             "inverse", "probit", "cloglog" or "sqrt".
      */
    def fromName(name: String): Link = {
      name match {
        case Identity.name => Identity
        case Logit.name => Logit
        case Log.name => Log
        case Inverse.name => Inverse
        case Probit.name => Probit
        case CLogLog.name => CLogLog
        case Sqrt.name => Sqrt
      }
    }
  }

  object Identity extends Link("identity") {

    override def link(mu: Double): Double = mu

    override def deriv(mu: Double): Double = 1.0

    override def unlink(eta: Double): Double = eta
  }

  object Logit extends Link("logit") {

    override def link(mu: Double): Double = math.log(mu / (1.0 - mu))

    override def deriv(mu: Double): Double = 1.0 / (mu * (1.0 - mu))

    override def unlink(eta: Double): Double = 1.0 / (1.0 + math.exp(-1.0 * eta))
  }

  object Log extends Link("log") {

    override def link(mu: Double): Double = math.log(mu)

    override def deriv(mu: Double): Double = 1.0 / mu

    override def unlink(eta: Double): Double = math.exp(eta)
  }

  object Inverse extends Link("inverse") {

    override def link(mu: Double): Double = 1.0 / mu

    override def deriv(mu: Double): Double = -1.0 * math.pow(mu, -2.0)

    override def unlink(eta: Double): Double = 1.0 / eta
  }

  object Probit extends Link("probit") {

    override def link(mu: Double): Double = dist.Gaussian(0.0, 1.0).icdf(mu)

    override def deriv(mu: Double): Double = {
      1.0 / dist.Gaussian(0.0, 1.0).pdf(dist.Gaussian(0.0, 1.0).icdf(mu))
    }

    override def unlink(eta: Double): Double = dist.Gaussian(0.0, 1.0).cdf(eta)
  }

  object CLogLog extends Link("cloglog") {

    override def link(mu: Double): Double = math.log(-1.0 * math.log(1 - mu))

    override def deriv(mu: Double): Double = 1.0 / ((mu - 1.0) * math.log(1.0 - mu))

    override def unlink(eta: Double): Double = 1.0 - math.exp(-1.0 * math.exp(eta))
  }

  object Sqrt extends Link("sqrt") {

    override def link(mu: Double): Double = math.sqrt(mu)

    override def deriv(mu: Double): Double = 1.0 / (2.0 * math.sqrt(mu))

    override def unlink(eta: Double): Double = eta * eta
  }
}

case class GeneralizedLinearRegressionModel(coefficients: Vector,
                                            intercept: Double,
                                            fal: FamilyAndLink) {
  def apply(features: Vector): Double = predict(features)

  def predictWithLink(features: Vector): (Double, Double) = {
    val eta = predictLink(features)
    (fal.fitted(eta), eta)
  }

  def predictLink(features: Vector): Double = {
    BLAS.dot(features, coefficients) + intercept
  }

  def predict(features: Vector): Double = {
    val eta = predictLink(features)
    fal.fitted(eta)
  }
}
