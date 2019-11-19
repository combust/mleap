package ml.combust.mleap.runtime.javadsl

import ml.combust.mleap.tensor.Tensor
import scala.collection.JavaConverters._

class TensorSupport {

  def toArray[T](tensor: Tensor[T]): java.util.List[T] = {
    tensor.toArray.toSeq.asJava
  }

}
