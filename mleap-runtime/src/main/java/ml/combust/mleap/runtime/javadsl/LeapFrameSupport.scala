package ml.combust.mleap.runtime.javadsl

import ml.combust.mleap.core.types.{StructField, StructType}
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}

import scala.collection.JavaConverters._

class LeapFrameSupport {

  def collect(frame: DefaultLeapFrame): java.util.List[Row] = {
    frame.collect().asJava
  }

  def select(frame: DefaultLeapFrame, fieldNames: java.util.List[String]): DefaultLeapFrame = {
    frame.select(fieldNames.asScala: _*).get
  }

  def drop(frame: DefaultLeapFrame, names: java.util.List[String]): DefaultLeapFrame = {
    frame.drop(names.asScala: _*).get
  }

  def getFields(structType: StructType): java.util.List[StructField]  = {
    structType.fields.asJava
  }
}
