package ml.combust.mleap.runtime.javadsl

import ml.combust.mleap.runtime.ArrayRow

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Created by hollinwilkins on 4/21/17.
  */
class LeapFrameBuilderSupport {
  def createRowFromIterable(iterable: java.lang.Iterable[Any]): ArrayRow = {
    val values = iterable.asScala.map {
      case s: java.util.List[_] => s.asScala
      case v => v
    }.toArray

    new ArrayRow(mutable.WrappedArray.make[Any](values))
  }
}
