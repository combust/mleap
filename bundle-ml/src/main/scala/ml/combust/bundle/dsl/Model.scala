package ml.combust.bundle.dsl

import ml.bundle.ModelDef.ModelDef
import ml.combust.bundle.serializer.HasBundleRegistry

/** Companion object for model.
  */
object Model {
  /** Create a dsl model from a bunle model.
    *
    * @param modelDef bundle model definition
    * @param hr bundle registry for custom types
    * @return dsl model
    */
  def fromBundle(modelDef: ModelDef)
                (implicit hr: HasBundleRegistry): Model = Model(op = modelDef.op,
    attributes = modelDef.attributes.map(AttributeList.fromBundle))
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
                 attributes: Option[AttributeList] = None) extends HasAttributeList[Model] {
  /** Convert to bundle model.
    *
    * @param hr bundle registry from custom types
    * @return bundle model definition
    */
  def asBundle(implicit hr: HasBundleRegistry): ModelDef = ModelDef(op = op,
    attributes = attributes.map(_.asBundle))

  override def replaceAttrList(list: Option[AttributeList]): Model = {
    copy(attributes = list)
  }
}
