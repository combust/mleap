package ml.combust.mleap.core.types

import scala.collection.immutable.ListMap

/**
  * Created by hollinwilkins on 7/3/17.
  */
case class Socket(port: String, name: String, dataType: DataType) {
  def asField: StructField = StructField(name, dataType)
}

object NodeShape {
  def apply(inputs: Seq[Socket], outputs: Seq[Socket]): NodeShape = {
    val imap = ListMap(inputs.map(s => (s.port, s)): _*)
    val omap = ListMap(outputs.map(s => (s.port, s)): _*)

    NodeShape(imap, omap)
  }

  def vector(inputSize: Int,
             outputSize: Int,
             inputBase: BasicType = BasicType.Double,
             outputBase: BasicType = BasicType.Double,
             inputPort: String = "input",
             outputPort: String = "output",
             inputCol: String = "input",
             outputCol: String = "output"): NodeShape = {
    NodeShape().withInput(inputPort, inputCol, TensorType(inputBase, Seq(inputSize))).
      withOutput(outputPort, outputCol, TensorType(outputBase, Seq(outputSize)))
  }

  def scalar(inputBase: BasicType = BasicType.Double,
             outputBase: BasicType = BasicType.Double,
             inputPort: String = "input",
             outputPort: String = "output",
             inputCol: String = "input",
             outputCol: String = "output"): NodeShape = {
    NodeShape().withInput(inputPort, inputCol, ScalarType(inputBase)).
      withOutput(outputPort, outputCol, ScalarType(outputBase))
  }

  def regression(featuresSize: Int,
                 base: BasicType = BasicType.Double,
                 featuresCol: String = "features",
                 predictionCol: String = "prediction"): NodeShape = {
    NodeShape().withInput("features", featuresCol, TensorType(base, Seq(featuresSize))).
      withOutput("prediction", predictionCol, ScalarType(base))
  }

  def basicClassifier(featuresSize: Int,
                      base: BasicType = BasicType.Double,
                      featuresCol: String = "features",
                      predictionCol: String = "prediction"): NodeShape = {
    NodeShape().withInput("features", featuresCol, TensorType(base, Seq(featuresSize))).
      withOutput("prediction", predictionCol, ScalarType(base))
  }

  def probabilisticClassifier(featuresSize: Int,
                              numClasses: Int,
                              base: BasicType = BasicType.Double,
                              featuresCol: String = "features",
                              predictionCol: String = "prediction",
                              rawPredictionCol: Option[String] = None,
                              probabilityCol: Option[String] = None): NodeShape = {
    var ns = NodeShape().withInput("features", featuresCol, TensorType(base, Seq(featuresSize)))

    for(rp <- rawPredictionCol) { ns = ns.withOutput("raw_prediction", rp, TensorType(base, Seq(numClasses))) }
    for(p <- probabilityCol) { ns = ns.withOutput("probability", p, TensorType(base, Seq(numClasses))) }

    ns.withOutput("prediction", predictionCol, ScalarType(base))
  }

  def basicCluster(featuresSize: Int,
                   base: BasicType = BasicType.Double,
                   outputType: DataType = ScalarType.Int,
                   featuresCol: String = "features",
                   predictionCol: String = "prediction"): NodeShape = {
    NodeShape().withInput("features", featuresCol, TensorType(base, Seq(featuresSize))).
      withOutput("prediction", predictionCol, outputType)
  }

  def probabilisticCluster(featuresSize: Int,
                           base: BasicType = BasicType.Double,
                           outputType: DataType = ScalarType.Int,
                           featuresCol: String = "features",
                           predictionCol: String = "prediction",
                           probabilityCol: Option[String] = None): NodeShape = {
    var ns = NodeShape().withInput("features", featuresCol, TensorType(base, Seq(featuresSize)))

    for(p <- probabilityCol) { ns = ns.withOutput("probability", p, ScalarType(base)) }

    ns.withOutput("prediction", predictionCol, outputType)
  }
}

case class NodeShape(inputs: ListMap[String, Socket] = ListMap(),
                     outputs: ListMap[String, Socket] = ListMap()) {
  def inputSchema: StructType = StructType(inputs.map(_._2.asField).toSeq).get
  def outputSchema: StructType = StructType(outputs.map(_._2.asField).toSeq).get

  def getInput(port: String): Option[StructField] = inputs.find(_._1 == port).map(_._2.asField)
  def getOutput(port: String): Option[StructField] = outputs.find(_._1 == port).map(_._2.asField)

  def input(port: String): StructField = getInput(port).get
  def output(port: String): StructField = getOutput(port).get

  def standardInput: StructField = input("input")
  def standardOutput: StructField = output("output")

  def withInput(port: String, name: String, dataType: DataType): NodeShape = {
    copy(inputs = inputs + (port -> Socket(port, name, dataType)))
  }

  def withOutput(port: String, name: String, dataType: DataType): NodeShape = {
    copy(outputs = outputs + (port -> Socket(port, name, dataType)))
  }

  def withStandardInput(name: String, dataType: DataType): NodeShape = {
    withInput("input", name, dataType)
  }

  def withStandardOutput(name: String, dataType: DataType): NodeShape = {
    withOutput("output", name, dataType)
  }
}
