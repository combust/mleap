package ml.combust.bundle.dsl

import ml.bundle.DataType.DataType
import ml.bundle.Socket.Socket

/** Companion object for holding constant values.
  */
object Shape {
  val standardInputPort: String = "input"
  val standardOutputPort: String = "output"

  /** Default constructor.
    *
    * @return empty shape
    */
  def apply(): Shape = new Shape(inputs = Seq(),
    outputs = Seq(),
    inputLookup = Map(),
    outputLookup = Map())

  /** Construct a shape with inputs and outputs.
    *
    * @param inputs input sockets
    * @param outputs output sockets
    * @return shape with inputs/outputs
    */
  def apply(inputs: Seq[Socket],
            outputs: Seq[Socket]): Shape = {
    val inputLookup = inputs.map(s => (s.port, s)).toMap
    val outputLookup = outputs.map(s => (s.port, s)).toMap

    new Shape(inputs = inputs,
      outputs = outputs,
      inputLookup = inputLookup,
      outputLookup = outputLookup)
  }

  /** Create a shape from a bundle shape.
    *
    * @param shape bundle shape
    * @return dsl shape
    */
  def fromBundle(shape: ml.bundle.Shape.Shape): Shape = Shape(inputs = shape.inputs,
    outputs = shape.outputs)
}

/** Class for holding the input fields and output fields of a [[Node]].
  * The shape also holds information for connecting the input/output fields
  * to the underlying ML model.
  *
  * A [[Shape]] contains input and output sockets. Sockets map field data
  * to certain functionality within a [[Model]]. For instance, say we want
  * to run a "label" field through a string indexer and have the result
  * output to the field "label_name". We could wire up the node like so:
  *
  * {{{
  * scala> import ml.bundle.dsl._
  * scala> Shape().withInput("label", "input"). // connect the "label" field to the model input
  *          withOutput("label_name", "output") // connect the model output to the "label_name" field
  * }}}
  *
  * Or more concisely:
  * {{{
  * scala> import ml.bundle.dsl._
  * scala> Shape().withStandardIO("label", "label_name") // shorthand for the above code
  * }}}
  *
  * @param inputs input sockets
  * @param outputs output sockets
  * @param inputLookup input sockets lookup by port
  * @param outputLookup output sockets lookup by port
  */
case class Shape private (inputs: Seq[Socket],
                          outputs: Seq[Socket],
                          inputLookup: Map[String, Socket],
                          outputLookup: Map[String, Socket]) {
  /** Convert to bundle shape.
    *
    * @return bundle shape
    */
  def asBundle: ml.bundle.Shape.Shape = ml.bundle.Shape.Shape(inputs = inputs,
    outputs = outputs)

  /** Get the standard input socket.
    *
    * The standard input socket is on port "input".
    *
    * @return standard input socket
    */
  def standardInput: Socket = input(Shape.standardInputPort)

  /** Get the standard output socket.
    *
    * The standard output socket is on port "output".
    *
    * @return standard output socket
    */
  def standardOutput: Socket = output(Shape.standardOutputPort)

  /** Add standard input/output sockets to the shape.
    *
    * This is the same as calling [[Shape#withStandardInput]] and
    * [[Shape#withStandardOutput]].
    *
    * @param nameInput name of the input socket
    * @param nameOutput name of the output socket
    * @return copy of the shape with standard input/output sockets added
    */
  def withStandardIO(nameInput: String, nameOutput: String): Shape = {
    withStandardInput(nameInput).withStandardOutput(nameOutput)
  }

  /** Add typed standard input/output sockets to the shape.
    *
    * This is the same as calling [[Shape#withStandardInput]] and
    * [[Shape#withStandardOutput]].
    *
    * @param nameInput name of the input socket
    * @param inputType type of the input socket
    * @param nameOutput name of the output socket
    * @param outputType type of the output socket
    * @return copy of the shape with standard input/output sockets added
    */
  def withStandardIO(nameInput: String, inputType: Option[DataType], nameOutput: String, outputType: Option[DataType]): Shape = {
    withStandardInput(nameInput, inputType).withStandardOutput(nameOutput, outputType)
  }

  /** Add standard input socket to the shape.
    *
    * @param name name of standard input socket
    * @return copy of the shape with standard input socket added
    */
  def withStandardInput(name: String): Shape = withInput(name, Shape.standardInputPort)

  /** Add typed standard input socket to the shape.
    *
    * @param name name of standard input socket
    * @param dataType type of standard input socket
    * @return copy of the shape with standard input socket added
    */
  def withStandardInput(name: String, dataType: Option[DataType]): Shape = withInput(name, Shape.standardInputPort, dataType)

  /** Add standard output socket to the shape.
    *
    * @param name name of standard output socket
    * @return copy of the shape with standard output socket added
    */
  def withStandardOutput(name: String): Shape = withOutput(name, Shape.standardOutputPort)

  /** Add typed standard output socket to the shape.
    *
    * @param name name of standard output socket
    * @param dataType type of standard output socket
    * @return copy of the shape with standard output socket added
    */
  def withStandardOutput(name: String, dataType: Option[DataType]): Shape = withOutput(name, Shape.standardOutputPort, dataType)

  /** Add an optional input socket to the shape.
    *
    * @param name optional name of input socket
    * @param port port of input socket
    * @return copy of the shape with input socket optionally added
    */
  def withInput(name: Option[String], port: String): Shape = {
    name.map(n => withInput(n, port)).getOrElse(this)
  }

  /** Add an optional output socket to the shape.
    *
    * @param name name of optional output socket
    * @param port port of output socket
    * @return copy of the shape with output socket optionally added
    */
  def withOutput(name: Option[String], port: String): Shape = {
    name.map(n => withOutput(n, port)).getOrElse(this)
  }

  /** Get the bundle protobuf shape.
    *
    * @return bundle protobuf shape
    */
  def bundleShape: ml.bundle.Shape.Shape = ml.bundle.Shape.Shape(inputs = inputs,
    outputs = outputs)

  /** Get an input by the port name. 
    *
    * @param port name of port 
    * @return socket for named port 
    */
  def input(port: String): Socket = inputLookup(port)

  /** Get an output by the port name. 
    *
    * @param port name of port 
    * @return socket for named port 
    */
  def output(port: String): Socket = outputLookup(port)

  /** Get an optional input by the port name. 
    *
    * @param port name of the port 
    * @return optional socket for the named port 
    */
  def getInput(port: String): Option[Socket] = inputLookup.get(port)

  /** Get an optional input by the port name. 
    *
    * @param port name of the port 
    * @return optional socket for the named port 
    */
  def getOutput(port: String): Option[Socket] = outputLookup.get(port)

  /** Add an input socket to the shape. 
    *
    * @param name name of input socket 
    * @param port port of input socket 
    * @return copy of the shape with input socket added 
    */
  def withInput(name: String, port: String): Shape = {
    require(!inputLookup.contains(port), s"input already exists for port: $port")
    val socket = Socket(name, port)
    val inputLookup2 = inputLookup + (port -> socket)
    copy(inputs = inputs :+ socket, inputLookup = inputLookup2)
  }

  /** Add a typed input socket to the shape. 
    *
    * @param name name of input socket 
    * @param port port of input socket 
    * @param dataType type of input socket
    * @return copy of the shape with input socket added 
    */
  def withInput(name: String, port: String, dataType: Option[DataType]): Shape = {
    require(!inputLookup.contains(port), s"input already exists for port: $port")
    val socket = Socket(name, port, dataType)
    val inputLookup2 = inputLookup + (port -> socket)
    copy(inputs = inputs :+ socket, inputLookup = inputLookup2)
  }

  /** Add an output socket to the shape. 
    *
    * @param name name of output socket 
    * @param port port of output socket 
    * @return copy of the shape with output socket added 
    */
  def withOutput(name: String, port: String): Shape = {
    require(!outputLookup.contains(port), s"output already exists for port: $port")
    val socket = Socket(name, port)
    val outputLookup2 = outputLookup + (port -> socket)
    copy(outputs = outputs :+ socket, outputLookup = outputLookup2)
  }

  /** Add a typed output socket to the shape. 
    *
    * @param name name of output socket 
    * @param port port of output socket 
    * @param dataType type of output socket
    * @return copy of the shape with output socket added 
    */
  def withOutput(name: String, port: String, dataType: Option[DataType]): Shape = {
    require(!outputLookup.contains(port), s"output already exists for port: $port")
    val socket = Socket(name, port, dataType)
    val outputLookup2 = outputLookup + (port -> socket)
    copy(outputs = outputs :+ socket, outputLookup = outputLookup2)
  }
}
