package ml.combust.bundle.dsl

import ml.bundle.Socket.Socket

/** Companion object for holding constant values.
  */
object Shape {
  val standardInputPort: String = "input"
  val standardOutputPort: String = "output"
}

/** Trait for read-only interface to [[Shape]] objects.
  *
  * Use this trait when desierializing [[Shape]] objects,
  * such as when reading a [[Node]] from a Bundle.ML file.
  */
trait ReadableShape {
  /** Create a protobuf shape.
    *
    * @return protobuf shape
    */
  def bundleShape: ml.bundle.Shape.Shape

  /** Get list of all inputs.
    *
    * @return list of inputs
    */
  def inputs: Seq[Socket]

  /** Get list of all outputs.
    *
    * @return list of all outputs
    */
  def outputs: Seq[Socket]

  /** Get an input by the port name.
    *
    * @param port name of port
    * @return socket for named port
    */
  def input(port: String): Socket

  /** Get an output by the port name.
    *
    * @param port name of port
    * @return socket for named port
    */
  def output(port: String): Socket

  /** Get an optional input by the port name.
    *
    * @param port name of the port
    * @return optional socket for the named port
    */
  def getInput(port: String): Option[Socket]

  /** Get an optional input by the port name.
    *
    * @param port name of the port
    * @return optional socket for the named port
    */
  def getOutput(port: String): Option[Socket]

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
}

/** Trait for writable interface to a [[Shape]].
  *
  * Use this trait when serializing a [[Shape]] object,
  * such as when writing a [[Node]] to a Bundle.ML file.
  */
trait WritableShape extends ReadableShape {
  /** Add standard input/output sockets to the shape.
    *
    * This is the same as calling [[WritableShape#withStandardInput]] and
    * [[WritableShape#withStandardOutput]].
    *
    * @param nameInput name of the input socket
    * @param nameOutput name of the output socket
    * @return copy of the shape with standard input/output sockets added
    */
  def withStandardIO(nameInput: String, nameOutput: String): this.type = {
    withStandardInput(nameInput).withStandardOutput(nameOutput)
  }

  /** Add standard input socket to the shape.
    *
    * @param name name of standard input socket
    * @return copy of the shape with standard input socket added
    */
  def withStandardInput(name: String): this.type = withInput(name, Shape.standardInputPort)

  /** Add standard output socket to the shape.
    *
    * @param name name of standard output socket
    * @return copy of the shape with standard output socket added
    */
  def withStandardOutput(name: String): this.type = withOutput(name, Shape.standardOutputPort)

  /** Add an input socket to the shape.
    *
    * @param name name of input socket
    * @param port port of input socket
    * @return copy of the shape with input socket added
    */
  def withInput(name: String, port: String): this.type

  /** Add an output socket to the shape.
    *
    * @param name name of output socket
    * @param port port of output socket
    * @return copy of the shape with output socket added
    */
  def withOutput(name: String, port: String): this.type
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
  * @param _shape protobuf shape object containing the shape information
  */
case class Shape(private var _shape: ml.bundle.Shape.Shape = ml.bundle.Shape.Shape(inputs = Seq(), outputs = Seq())) extends WritableShape {
  private var inputLookup: Map[String, Socket] = _shape.inputs.map(s => (s.port, s)).toMap
  private var outputLookup: Map[String, Socket] = _shape.outputs.map(s => (s.port, s)).toMap

  override def bundleShape: ml.bundle.Shape.Shape = _shape

  override def inputs: Seq[Socket] = _shape.inputs
  override def outputs: Seq[Socket] = _shape.outputs

  override def input(port: String): Socket = inputLookup(port)
  override def output(port: String): Socket = outputLookup(port)

  override def getInput(port: String): Option[Socket] = inputLookup.get(port)
  override def getOutput(port: String): Option[Socket] = outputLookup.get(port)

  override def withInput(name: String, port: String): Shape.this.type = {
    if(inputLookup.contains(port)) { throw new Error("only one input allowed per port") } // TODO: better error
    val socket = Socket(name, port)
    inputLookup = inputLookup + (port -> socket)
    _shape = _shape.copy(inputs = _shape.inputs :+ socket)
    this
  }
  override def withOutput(name: String, port: String): Shape.this.type = {
    if(outputLookup.contains(port)) { throw new Error("only one output allowed per port") } // TODO: better error
    val socket = Socket(name, port)
    outputLookup = outputLookup + (port -> socket)
    _shape = _shape.copy(outputs = _shape.outputs :+ socket)
    this
  }
}
