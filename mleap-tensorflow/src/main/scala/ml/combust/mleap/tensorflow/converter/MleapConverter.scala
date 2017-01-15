package ml.combust.mleap.tensorflow.converter

import java.util

import ml.combust.mleap.core.tensor.Tensor
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
    case StringType(isNullable) =>
      tensorflow.Tensor.create(value.asInstanceOf[String].getBytes("UTF-8"))
    case tt: TensorType =>
      tt.base match {
        case StringType(false) =>
          throw new RuntimeException(s"unsupported tensorflow type: $dataType")
        case _ =>
          val tensor = value.asInstanceOf[Tensor[_]]
          val values = tensor.toDense.values
          val copy = copyArray(tt.base)

          val nested = if(tensor.dimensions.last == -1) {
            copy(values, 0, values.length)
          } else {
            deepCopy(values, tensor.dimensions, 0)(copy)._1
          }

          tensorflow.Tensor.create(nested)
      }
    case _ =>
      throw new RuntimeException(s"unsupported tensorflow type: $dataType")
  }

  def deepCopy(values: AnyRef, dimensions: Seq[Int], index: Int)
              (copy: (AnyRef, Int, Int) => AnyRef): (AnyRef, Int) = dimensions match {
    case head :: Nil =>
      val nextIndex = index + head
      val arr = copy(values, index, nextIndex)
      (arr, nextIndex)
    case head :: tail =>
      val arr = new Array[AnyRef](head)
      val newIndex = (0 until head).foldLeft(index) {
        case (ind, i) =>
          val (a1, i1) = deepCopy(values, tail, ind)(copy)
          arr(i) = a1
          i1
      }
      (arr, newIndex)
  }

  def copyArray(base: BasicType): (AnyRef, Int, Int) => AnyRef = (base match {
    case FloatType(false) =>
      (arr: Array[Float], from: Int, to: Int) =>
        util.Arrays.copyOfRange(arr, from, to)
    case DoubleType(false) =>
      (arr: Array[Double], from: Int, to: Int) =>
        util.Arrays.copyOfRange(arr, from, to)
    case IntegerType(false) =>
      (arr: Array[Int], from: Int, to: Int) =>
        util.Arrays.copyOfRange(arr, from, to)
    case LongType(false) =>
      (arr: Array[Long], from: Int, to: Int) =>
        util.Arrays.copyOfRange(arr, from, to)
    case BooleanType(false) =>
      (arr: Array[Boolean], from: Int, to: Int) =>
        util.Arrays.copyOfRange(arr, from, to)
  }).asInstanceOf[(AnyRef, Int, Int) => AnyRef]
}
