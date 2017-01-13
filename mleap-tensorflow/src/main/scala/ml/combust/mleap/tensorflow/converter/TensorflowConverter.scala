package ml.combust.mleap.tensorflow.converter

import ml.combust.mleap.core.DenseTensor
import ml.combust.mleap.runtime.types._
import org.tensorflow

/**
  * Created by hollinwilkins on 1/12/17.
  */
object TensorflowConverter {
  def convert(tensor: tensorflow.Tensor, dataType: DataType): Any = dataType match {
    case FloatType(isNullable) =>
      val v = tensor.floatValue()
      if(isNullable) Some(v) else v
    case DoubleType(isNullable) =>
      val v = tensor.doubleValue()
      if(isNullable) Some(v) else v
    case IntegerType(isNullable) =>
      val v = tensor.intValue()
      if(isNullable) Some(v) else v
    case LongType(isNullable) =>
      val v = tensor.longValue()
      if(isNullable) Some(v) else v
    case BooleanType(isNullable) =>
      val v = tensor.booleanValue()
      if(isNullable) Some(v) else v
    case StringType(isNullable) =>
      val v = new String(tensor.bytesValue(), "UTF-8")
      if(isNullable) Some(v) else v
    case tt: TensorType =>
      val shape = tensor.shape().map(_.toInt).toSeq
      val arrF: (Int) => Array[_] = tt.base match {
        case FloatType(false) =>
          (i) => new Array[Float](i)
        case DoubleType(false) =>
          (i) => new Array[Double](i)
        case IntegerType(false) =>
          (i) => new Array[Integer](i)
        case LongType(false) =>
          (i) => new Array[Long](i)
        case BooleanType(false) =>
          (i) => new Array[Boolean](i)
        case StringType(false) =>
          throw new RuntimeException(s"unsupported tensorflow type: $dataType")
      }

      DenseTensor(createArray(shape, arrF), shape)
    case _ =>
      throw new RuntimeException(s"unsupported tensorflow type: $dataType")
  }

  private def createArray(shape: Seq[Int],
                          f: (Int) => Array[_]): Array[_] = shape match {
    case head :: Nil => f(head)
    case head :: tail => Array.tabulate[Array[_]](head)(_ => createArray(tail, f))
  }
}
