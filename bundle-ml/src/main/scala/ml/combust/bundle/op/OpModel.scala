package ml.combust.bundle.op

import ml.combust.bundle.serializer.BundleContext
import ml.combust.bundle.dsl.{Model, ReadableModel, WritableModel}

/** Type class for serializing/deserializing ML models to Bundle.ML.
  *
  * @tparam M Scala class of the ML model
  */
trait OpModel[M] {
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
  def store(context: BundleContext,
            model: WritableModel,
            obj: M): WritableModel

  /** Load the model.
    *
    * Load all standard parameters from the model attributes.
    * Load all non-standard parameters like decision trees from the custom files.
    *
    * @param context bundle context for decoding custom values and reading non-standard files
    * @param model model and attributes read from Bundle.ML
    * @return reconstructed ML model from the model and context
    */
  def load(context: BundleContext,
           model: ReadableModel): M
}
