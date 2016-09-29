package ml.combust.bundle

/** Domain specific language for generating Bundle.ML models.
  *
  * DSL provides objects for generating various Bundle.ML objects.
  *
  * [[ml.combust.bundle.dsl.Bundle]] - Stores the name, version,
  *   serialization format, list of attributes, and list of root
  *   nodes in the bundle graph.
  *
  * [[ml.combust.bundle.dsl.Node]] - A node for transforming data in the
  *   model graph. Nodes have a unique name within a Bundle.ML graph
  *   and are responsible for piping inputs to a model and outputs
  *   from a model to the rest of the graph via sockets. Sockets
  *   map a field name in the graph to a specific port in a model.
  *
  *   A model can have input ports and output ports. An example would
  *   be a StringIndexer node named "my_string_indexer" that indexes
  *   the input field "my_string_label" to the output field
  *   "my_double_label". There are two sockets for this node, one that
  *   maps the "my_string_label" field to the "input" port of the model
  *   and another socket that maps the "output" port of the model to
  *   the "my_double_label" field.
  *
  * [[ml.combust.bundle.dsl.Model]] - A model operation that transforms data
  *   within the graph. Models have an op name that define the type of
  *   model. For instance, the op name could be "string_indexer",
  *   "linear_regression", or "random_forest_regression". In addition
  *   to the op name of the model, all of the required attributes,
  *   such as coefficients and intercepts ,for executing the model are
  *   stored here.
  *
  * [[ml.combust.bundle.dsl.Attribute]] - Attributes are used to store values for
  *   [[ml.combust.bundle.dsl.Model]]s and [[ml.combust.bundle.dsl.Bundle]]s. They contain
  *   a name, a [[ml.bundle.DataType.DataType]] and a [[ml.combust.bundle.dsl.Value]].
  *
  * [[ml.combust.bundle.dsl.Value]] - Values store concrete data for an [[ml.combust.bundle.dsl.Attribute]].
  *   You can store basic data like strings, doubles, longs and booleans,
  *   tensors of any dimension, custom data types, and lists of any depth and
  *   data type.
  *
  * [[ml.combust.bundle.dsl.AttributeList]] - Stores a set of [[ml.combust.bundle.dsl.Attribute]]s.
  *
  * [[ml.combust.bundle.dsl.Shape]] - Stores the map of graph input/output fields to
  *   model inputs and outputs.
  */
package object dsl { }
