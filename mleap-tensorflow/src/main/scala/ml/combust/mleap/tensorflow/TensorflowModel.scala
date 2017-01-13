package ml.combust.mleap.tensorflow

import java.io.Closeable

import ml.combust.mleap.runtime.types.DataType
import ml.combust.mleap.tensorflow.converter.{MleapConverter, TensorflowConverter}
import org.bytedeco.javacpp.tensorflow

/**
  * Created by hollinwilkins on 1/12/17.
  */
case class TensorflowModel(graph: tensorflow.GraphDef,
                           inputs: Seq[(String, DataType)],
                           outputs: Seq[(String, DataType)],
                           nodes: Option[Seq[String]] = None) extends Closeable {
  @transient
  private var session: Option[tensorflow.Session] = None

  @transient
  private val inputNames: Array[String] = inputs.map(_._1).toArray

  @transient
  private val outputVector: tensorflow.StringVector = new tensorflow.StringVector(outputs.map(_._1): _*)

  @transient
  private val outputNodes: tensorflow.StringVector = nodes.map(s => new tensorflow.StringVector(s: _*)).getOrElse(new tensorflow.StringVector())

  def apply(values: Any *): Seq[Any] = {
    val tensors = values.zip(inputs).map {
      case (v, (_, dataType)) => MleapConverter.convert(v, dataType)
    }
    val inputMap = new tensorflow.StringTensorPairVector(inputNames, tensors.toArray)
    val outputTensors = new tensorflow.TensorVector()

    withSession {
      session =>
        graph.node(0).op()
        if(session.Run(inputMap, outputVector, outputNodes, outputTensors).ok()) {
          val convertedOutputs = new Array[Any](outputTensors.size().toInt)
          var i = 0
          while(i < outputTensors.size()) {
            convertedOutputs(i) = TensorflowConverter.convert(outputTensors.get(i), outputs(i)._2)
            i += 1
          }
          convertedOutputs
        } else {
          throw new RuntimeException("could not execute tensorflow graph")
        }
    }
  }

  private def withSession[T](f: (tensorflow.Session) => T): T = {
    val s = session.getOrElse {
      val s = new tensorflow.Session(new tensorflow.SessionOptions())

      if(s.Create(graph).ok()) {
        session = Some(s)
        s
      } else {
        throw new RuntimeException("could not initialize tensorflow graph")
      }
    }

    f(s)
  }

  override def close(): Unit = {
    session.foreach(_.Close())
  }
}
