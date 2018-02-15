package ml.combust.mleap.core.sklearn

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.types.{StructType, TensorType}
import org.apache.spark.ml.linalg.{Vector, Vectors}

/**
  * Calculates the polynomial expansion according to the given combinations.
  *
  * */
case class PolynomialFeaturesModel(combinations: String) extends Model {

  private val pattern = "x(\\d+)(?:[\\^](\\d+))?".r
  private val polynomials = extractPolynomials(combinations)
  private val indices = polynomials.flatMap(poly => poly.terms).map(term => term.index).toSet

  private def extractPolynomials(combinations: String): List[Polynomial] = {
    combinations.split(",")
                .map(combination => extractPolynomial(combination))
                .toList
  }

  private def extractPolynomial(polynomial: String): Polynomial = {
    Polynomial(pattern.findAllIn(polynomial).matchData
      .map(matcher => {Term(matcher.group(1).toInt, Option(matcher.group(2)).getOrElse("1").toInt)})
      .toList
    )
  }

  def getPolyValue(poly: Polynomial, features: Vector): Double = {
    poly.terms.map(term => scala.math.pow(features(term.index), term.power)).product
  }

  def apply(features: Vector): Vector = {
    Vectors.dense(polynomials.map(poly => getPolyValue(poly, features)).toArray)
  }

  override def inputSchema: StructType = StructType("input" -> TensorType.Double(indices.size)).get

  override def outputSchema: StructType = StructType("output" -> TensorType.Double(polynomials.size)).get
}

case class Term(index: Int, power: Int)

case class Polynomial(terms: List[Term])