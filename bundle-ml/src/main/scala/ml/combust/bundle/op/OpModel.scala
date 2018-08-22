package ml.combust.bundle.op

import ml.combust.bundle.BundleContext
import ml.combust.bundle.dsl.Model

/** Type class for serializing/deserializing ML models to Bundle.ML.
  *
  * @tparam Context context for implementation
  * @tparam M Scala class of the ML model
  */
trait OpModel[Context, M] {
  /** Class of the model.
    */
  val klazz: Class[M]

  /** Get the name of the model.
    *
    * @return name of the model
    */
  def opName: String

  /** Get the name of the model.
    *
    * This version uses the bundle context, in case the name is being
    * deferred to another op.
    *
    * Defaults to returning the opName.
    *
    * @param obj the model
    * @param context bundle context
    * @return
    */
  def modelOpName(obj: M)
                 (implicit context: BundleContext[Context]): String = opName

  /** Store the model.
    *
    * Store all standard parameters to the model's attribute list.
    * Store all non-standard parameters like a decision tree to files.
    *
    * Attributes saved to the writable model will be serialized for you
    * to JSON or Protobuf depending on the selected [[ml.combust.bundle.serializer.SerializationFormat]].
    *
    * @param model writable model to store model attributes in
    * @param obj object to be stored in Bundle.ML
    * @param context bundle context for custom types
    * @return writable model to be serialized
    */
  def store(model: Model, obj: M)
           (implicit context: BundleContext[Context]): Model

  /** Load the model.
    *
    * Load all standard parameters from the model attributes.
    * Load all non-standard parameters like decision trees from the custom files.
    *
    * @param model model and attributes read from Bundle.ML
    * @param context bundle context for custom types
    * @return reconstructed ML model from the model and context
    */
  def load(model: Model)
          (implicit context: BundleContext[Context]): M
}
