package ml.combust.mleap.runtime

import ml.combust.mleap.core.Model
import ml.combust.mleap.core.types.{DataType, TensorType}
import ml.combust.mleap.runtime.function.UserDefinedFunction

object SchemaUtil {

  def asssertModelTypesMatchTransformerTypes(model: Model, exec: UserDefinedFunction): Unit = {
    SchemaUtil.checkTypes(model.inputSchema.fields.map(field => field.dataType),
      exec.inputs.map(in => in.dataTypes).flatten)
    SchemaUtil.checkTypes(model.outputSchema.fields.map(field => field.dataType),
      exec.output.dataTypes)
  }

  private def checkTypes(modelTypes: Seq[DataType], transformerTypes: Seq[DataType]) = {
    assert(modelTypes.size == modelTypes.size)
    modelTypes.zip(transformerTypes).foreach {
      case (modelType, transformerType) => {
        if (modelType.isInstanceOf[TensorType]) {
          assert(transformerType.isInstanceOf[TensorType] &&
            modelType.base == transformerType.base)
        } else {
          assert(modelType == transformerType)
        }
      }
    }
  }
}
