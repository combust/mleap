package ml.combust.mleap.core.types

import scala.collection.immutable.ListMap

/**
  * Created by hollinwilkins on 7/3/17.
  */
case class Socket(port: String, name: String)

object NodeShape {
  def apply(inputs: Seq[Socket], outputs: Seq[Socket]): NodeShape = {
    val imap = ListMap(inputs.map(s => (s.port, s)): _*)
    val omap = ListMap(outputs.map(s => (s.port, s)): _*)

    NodeShape(imap, omap)
  }

  def feature(inputPort: String = "input",
              outputPort: String = "output",
              inputCol: String = "input",
              outputCol: String = "output"): NodeShape = {
    NodeShape().withInput(inputPort, inputCol).
          withOutput(outputPort, outputCol)
  }

  def regression(featuresCol: String = "features",
                 predictionCol: String = "prediction"): NodeShape = {
    NodeShape().withInput("features", featuresCol).
          withOutput("prediction", predictionCol)
  }

  def basicClassifier(featuresCol: String = "features",
                      predictionCol: String = "prediction"): NodeShape = {
    NodeShape().withInput("features", featuresCol).
          withOutput("prediction", predictionCol)
  }

  def probabilisticClassifier(featuresCol: String = "features",
                              predictionCol: String = "prediction",
                              rawPredictionCol: Option[String] = None,
                              probabilityCol: Option[String] = None): NodeShape = {
    var ns = NodeShape().withInput("features", featuresCol)

    for(rp <- rawPredictionCol) { ns = ns.withOutput("raw_prediction", rp) }
    for(p <- probabilityCol) { ns = ns.withOutput("probability", p) }

    ns.withOutput("prediction", predictionCol)
  }

  def basicCluster(featuresCol: String = "features",
                   predictionCol: String = "prediction"): NodeShape = {
    NodeShape().withInput("features", featuresCol).
          withOutput("prediction", predictionCol)
  }

  def probabilisticCluster(featuresCol: String = "features",
                           predictionCol: String = "prediction",
                           probabilityCol: Option[String] = None): NodeShape = {
    var ns = NodeShape().withInput("features", featuresCol)

    for(p <- probabilityCol) { ns = ns.withOutput("probability", p) }

    ns.withOutput("prediction", predictionCol)
  }
}

case class NodeShape(inputs: ListMap[String, Socket] = ListMap(),
                     outputs: ListMap[String, Socket] = ListMap()) {
  def getInput(port: String): Option[Socket] = inputs.find(_._1 == port).map(_._2)
  def getOutput(port: String): Option[Socket] = outputs.find(_._1 == port).map(_._2)

  def input(port: String): Socket = getInput(port).get
  def output(port: String): Socket = getOutput(port).get

  def standardInput: Socket = input("input")
  def standardOutput: Socket = output("output")

  def withInput(port: String, name: String): NodeShape = {
    copy(inputs = inputs + (port -> Socket(port, name)))
  }

  def withOutput(port: String, name: String): NodeShape = {
    copy(outputs = outputs + (port -> Socket(port, name)))
  }

  def withStandardInput(name: String): NodeShape = {
    withInput("input", name)
  }

  def withStandardOutput(name: String): NodeShape = {
    withOutput("output", name)
  }
}
