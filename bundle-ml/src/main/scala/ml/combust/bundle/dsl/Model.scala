package ml.combust.bundle.dsl

import ml.bundle

/** Companion object for model.
  */
object Model {
  /** Create a dsl model from a bunle model.
    *
    * @param model bundle model definition
    * @return dsl model
    */
  def fromBundle(model: bundle.Model): Model = Model(op = model.op,
    attributes = Attributes.fromBundle(model.attributes.get))
}

/** Class that encodes all information need to serialize or deserialize
  * a machine learning model.
  *
  * Models encode things like coefficients for linear regressions,
  * labels for string indexers, sizes for one hot encoders, decision trees,
  * and any other data needed to serialize and deserialize ML models and
  * feature builders.
  *
  * @param op op name for the model
  * @param attributes optional list of attributes for the model
  */
case class Model(op: String,
                 attributes: Attributes = Attributes()) extends HasAttributes[Model] {
  /** Convert to bundle model.
    *
    * @return bundle model definition
    */
  def asBundle: bundle.Model = bundle.Model(op = op,
    attributes = Some(attributes.asBundle))

  override def withAttributes(attrs: Attributes): Model = {
    copy(attributes = attrs)
  }
}
