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

  /** Store the model.
    *
    * Store all standard parameters to the model's attribute list.
    * Store all non-standard parameters like a decision tree to files.
    *
    * Attributes saved to the writable model will be serialized for you
    * to JSON or Protobuf depending on the selected [[ml.combust.bundle.serializer.SerializationFormat]].
    *
    * @param context bundle context for encoding custom values and writing non-standard files
    * @param model writable model to store model attributes in
    * @param obj object to be stored in Bundle.ML
    * @return writable model to be serialized
    */
  def store(context: BundleContext[Context],
            model: Model,
            obj: M): Model

  /** Load the model.
    *
    * Load all standard parameters from the model attributes.
    * Load all non-standard parameters like decision trees from the custom files.
    *
    * @param context bundle context for decoding custom values and reading non-standard files
    * @param model model and attributes read from Bundle.ML
    * @return reconstructed ML model from the model and context
    */
  def load(context: BundleContext[Context],
           model: Model): M
}
