package org.apache.spark.ml.bundle

import ml.bundle.Socket
import ml.combust.bundle.dsl.NodeShape
import org.apache.spark.ml.param.{Param, Params, StringArrayParam}
import org.apache.spark.sql.DataFrame

import scala.language.implicitConversions

/**
  * Created by hollinwilkins on 7/4/17.
  */
object ParamSpec {
  implicit def apply(t: (String, Param[String])): SimpleParamSpec = SimpleParamSpec(t._1, t._2)
  implicit def apply(t: (String, StringArrayParam)): ArrayParamSpec = ArrayParamSpec(t._1, t._2)
}
sealed trait ParamSpec
case class SimpleParamSpec(port: String, param: Param[String]) extends ParamSpec
case class ArrayParamSpec(portPrefix: String, param: StringArrayParam) extends ParamSpec

object SparkShapeSaver {
  def apply(params: Params)
           (implicit dataset: DataFrame): SparkShapeSaver = {
    SparkShapeSaver(dataset, params, Seq(), Seq())
  }
}

case class SparkShapeSaver(dataset: DataFrame,
                           params: Params,
                           inputs: Seq[ParamSpec],
                           outputs: Seq[ParamSpec]) {
  private implicit val ds = dataset

  def withInputs(is: ParamSpec *): SparkShapeSaver = {
    copy(inputs = inputs ++ is)
  }

  def withOutputs(os: ParamSpec *): SparkShapeSaver = {
    copy(outputs = outputs ++ os)
  }

  def asNodeShape: NodeShape = {
    val is = inputs.flatMap {
      case SimpleParamSpec(port, param) =>
        if(params.isDefined(param) && params.getOrDefault(param).nonEmpty) {
          val field = dataset.schema(params.getOrDefault(param))
          Seq(Socket(port, field.name))
        }
        else { Seq() }
      case ArrayParamSpec(portPrefix, param) =>
        if(params.isDefined(param) && params.getOrDefault(param).nonEmpty) {
          params.get(param).get.zipWithIndex.map {
            case (name, i) =>
              val field = dataset.schema(name)
              Socket(s"$portPrefix$i", field.name)
          }.toSeq
        } else { Seq() }
    }

    val os = outputs.flatMap {
      case SimpleParamSpec(port, param) =>
        if(params.isDefined(param) && params.getOrDefault(param).nonEmpty) {
          val field = dataset.schema(params.getOrDefault(param))
          Seq(Socket(port, field.name))
        }
        else { Seq() }
      case ArrayParamSpec(portPrefix, param) =>
        if(params.isDefined(param) && params.getOrDefault(param).nonEmpty) {
          params.get(param).get.zipWithIndex.map {
            case (name, i) =>
              val field = dataset.schema(name)
              Socket(s"$portPrefix$i", field.name)
          }.toSeq
        } else { Seq() }
    }

    NodeShape(inputs = is, outputs = os)
  }
}

case class SparkShapeLoader(shape: NodeShape,
                            params: Params,
                            inputs: Seq[ParamSpec] = Seq(),
                            outputs: Seq[ParamSpec] = Seq()) {
  def withInputs(is: ParamSpec *): SparkShapeLoader = {
    copy(inputs = inputs ++ is)
  }

  def withOutputs(os: SimpleParamSpec *): SparkShapeLoader = {
    copy(outputs = outputs ++ os)
  }

  def loadShape(): Unit = {
    for(input <- inputs) {
      input match {
        case SimpleParamSpec(port, param) =>
          for(socket <- shape.getInput(port)) {
            params.set(param, socket.name)
          }
        case ArrayParamSpec(portPrefix, param) =>
          val names = shape.inputs.filter(_.port.startsWith(portPrefix)).map(_.name).toArray
          params.set(param, names)
      }
    }

    for(output ← outputs) {
      output match {
        case SimpleParamSpec(port, param) =>
          for(socket <- shape.getOutput(port)) {
            params.set(param, socket.name)
          }
        case ArrayParamSpec(portPrefix, param) =>
          val names = shape.outputs.filter(_.port.startsWith(portPrefix)).map(_.name).toArray
          params.set(param, names)
      }
    }
  }
}
