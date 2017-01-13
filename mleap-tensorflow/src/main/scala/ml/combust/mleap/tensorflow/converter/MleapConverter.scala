package ml.combust.mleap.tensorflow.converter

import ml.combust.mleap.core.Tensor
import ml.combust.mleap.runtime.types._
import org.tensorflow

/**
  * Created by hollinwilkins on 1/12/17.
  */
object MleapConverter {
  def convert(value: Any, dataType: DataType): tensorflow.Tensor = dataType match {
    case FloatType(isNullable) =>
      tensorflow.Tensor.create(value.asInstanceOf[Float])
    case DoubleType(isNullable) =>
      tensorflow.Tensor.create(value.asInstanceOf[Double])
    case IntegerType(isNullable) =>
      tensorflow.Tensor.create(value.asInstanceOf[Int])
    case LongType(isNullable) =>
      tensorflow.Tensor.create(value.asInstanceOf[Long])
    case BooleanType(isNullable) =>
      tensorflow.Tensor.create(value.asInstanceOf[Boolean])
    case tt: TensorType =>
      tensorflow.Tensor.create(value.asInstanceOf[Tensor[_]].toDense.values)
    case _ =>
      throw new RuntimeException(s"unsupported tensorflow type: $dataType")
  }
}
