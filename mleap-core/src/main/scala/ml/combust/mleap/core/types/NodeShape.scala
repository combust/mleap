package ml.combust.mleap.core.types

/**
  * Created by hollinwilkins on 7/3/17.
  */
case class Socket(port: String, field: StructField)

case class NodeShape(inputs: Seq[Socket],
                     outputs: Seq[Socket]) {
  def inputSchema: StructType = StructType(inputs.map(_.field)).get
  def outputSchema: StructType = StructType(outputs.map(_.field)).get

  def getInput(port: String): Option[StructField] = inputs.find(_.port == port).map(_.field)
  def getOutput(port: String): Option[StructField] = outputs.find(_.port == port).map(_.field)

  def input(port: String): StructField = getInput(port).get
  def output(port: String): StructField = getOutput(port).get
}
