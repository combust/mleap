package ml.combust.bundle

/** Provides traits and objects for handling Bundle.ML JSON data with Spray JSON.
  *
  * == Overview ==
  * [[ml.combust.bundle.json.JsonSupport]] provides all spray.json.JsonFormat formats.
  * Either import or mixin the formats into scope in order to use them.
  * {{{
  * scala> import ml.bundle.json.JsonSupport._
  *        import spray.json._
  *        import ml.bundle._
  *        val tt = TensorType.TensorType(BasicType.BasicType.DOUBLE, Seq(-1))
  *        val underlying = DataType.DataType.Underlying.Tensor(tt)
  *        val dataType = DataType.DataType(underlying)
  * scala> val jsonString = dataType.toJson.prettyPrint
  * jsonString: String =
  * {
  *   "type": "tensor",
  *   "tensor": {
  *   "base": "double",
  *   "dimensions": [-1]
  *   }
  * }
  * }}}
  */
package object json { }
