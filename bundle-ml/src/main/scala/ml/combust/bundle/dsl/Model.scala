package ml.combust.bundle.dsl

import ml.bundle.ModelDef.ModelDef
import ml.combust.bundle.serializer.SerializationContext

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
  /** Convert the [[Model]] to a protobuf serializable object.
    *
    * @param context serialization context for encoding custom values
    * @return protobuf model definition
    */
  def bundleModel(implicit context: SerializationContext): ModelDef = {
    ModelDef(op = op,
      attributes = attributes.map(_.bundleList))
  }

  override def replaceAttrList(list: Option[AttributeList]): Model = {
    copy(attributes = list)
  }
}
