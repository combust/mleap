package ml.combust.mleap.runtime.javadsl

import ml.combust.mleap.core.types.{StructField, StructType}
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row}

import scala.jdk.CollectionConverters._

class LeapFrameSupport {

  def collect(frame: DefaultLeapFrame): java.util.List[Row] = {
    frame.collect().asJava
  }

  def select(frame: DefaultLeapFrame, fieldNames: java.util.List[String]): DefaultLeapFrame = {
    frame.select(fieldNames.asScala.toSeq: _*).get
  }

  def drop(frame: DefaultLeapFrame, names: java.util.List[String]): DefaultLeapFrame = {
    frame.drop(names.asScala.toSeq: _*).get
  }

  def getFields(schema: StructType): java.util.List[StructField]  = {
    schema.fields.asJava
  }
}
