package ml.combust.mleap.executor

import java.net.URI

import akka.stream.ThrottleMode
import ml.combust.mleap.core.types.StructType
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row, Transformer}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Try}

/** Used to execute a frame transform operation.
  *
  */
object ExecuteTransform {
  /** Uses a transformer to transform a leap frame.
    *
    * @param transformer transformer to run
    * @param frame leap frame
    * @param options transform options
    * @param ec execution context
    * @return a future of the transformed leap frame
    */
  def apply(transformer: Transformer,
            frame: DefaultLeapFrame,
            options: TransformOptions)
           (implicit ec: ExecutionContext): Future[Try[DefaultLeapFrame]] = {
    transformer.transformAsync(frame).
      map(Try(_)).recover {
      case err => Failure(err)
    }.map {
      tFrame =>
        tFrame.flatMap {
          frame =>
            options.select.map {
              s =>
                options.selectMode match {
                  case SelectMode.Strict => frame.select(s: _*)
                  case SelectMode.Relaxed => Try(frame.relaxedSelect(s: _*))
                }
            }.getOrElse(tFrame)
        }
    }
  }

  /** Uses a transformer to transform a leap frame.
    *
    * @param transformer transformer to run
    * @param tFrame try leap frame
    * @param options transform options
    * @param ec execution context
    * @return a future of the transformed leap frame
    */
  def apply(transformer: Transformer,
            tFrame: Try[DefaultLeapFrame],
            options: TransformOptions)
           (implicit ec: ExecutionContext): Future[Try[DefaultLeapFrame]] = {
    Future.fromTry {
      tFrame.map {
        frame =>
          transformer.transformAsync(frame).map {
            frame =>
              options.select.map {
                s =>
                  options.selectMode match {
                    case SelectMode.Strict => frame.select(s: _*)
                    case SelectMode.Relaxed => Try(frame.relaxedSelect(s: _*))
                  }
              }.getOrElse(Try(frame))
          }
      }
    }.flatten
  }
}

case class Throttle(elements: Int,
                    duration: FiniteDuration,
                    maxBurst: Int,
                    mode: ThrottleMode)

/** Specifies options for streams of transforms.
  *
  * @param idleTimeout timeout for stream inactivity
  * @param transformDelay delay to add to transform operation
  * @param parallelism parallelism of transforms
  * @param throttle optionally throttle the stream
  * @param bufferSize size of buffer for transform elements
  */
case class StreamConfig(idleTimeout: Option[FiniteDuration] = None,
                        transformDelay: Option[FiniteDuration] = None,
                        parallelism: Option[Parallelism] = None,
                        throttle: Option[Throttle] = None,
                        bufferSize: Option[Int] = None)

/** Specifies options for streams of transforms.
  *
  * @param idleTimeout timeout for stream inactivity
  * @param transformDelay delay to add to transform operation
  * @param parallelism parallelism of transforms
  * @param throttle optionally throttle the stream
  */
case class FlowConfig(idleTimeout: Option[FiniteDuration] = None,
                      transformDelay: Option[FiniteDuration] = None,
                      parallelism: Option[Parallelism] = None,
                      throttle: Option[Throttle] = None)

/** Specifies the schema and transform options for
  * a row transformer.
  *
  * @param schema input schema of the rows
  * @param options transform options to apply for transform
  */
case class RowStreamSpec(schema: StructType,
                         options: TransformOptions = TransformOptions.default)

sealed trait ModelRequest {
  def modelName: String
}

/** Request to transform a leap frame.
  *
  * @param modelName name of the model
  * @param frame leap frame to transform
  * @param options transform options
  */
case class TransformFrameRequest(modelName: String,
                                 frame: DefaultLeapFrame,
                                 options: TransformOptions = TransformOptions.default) extends ModelRequest

case class StreamTransformFrameRequest(frame: Try[DefaultLeapFrame],
                                       options: TransformOptions)
case class StreamTransformRowRequest(row: Try[Row])

case class Model(name: String,
                 uri: URI,
                 config: ModelConfig)

case class FrameStream(modelName: String,
                       streamName: String,
                       streamConfig: StreamConfig)

case class RowStream(modelName: String,
                     streamName: String,
                     streamConfig: StreamConfig,
                     spec: RowStreamSpec,
                     outputSchema: StructType)

object ModelConfig {
  lazy val default: ModelConfig = ModelConfig()
}
case class ModelConfig(memoryTimeout: Option[FiniteDuration] = None,
                       diskTimeout: Option[FiniteDuration] = None)

case class LoadModelRequest(modelName: String,
                            uri: URI,
                            config: Option[ModelConfig] = None,
                            force: Boolean = false) extends ModelRequest

case class GetBundleMetaRequest(modelName: String) extends ModelRequest

case class GetModelRequest(modelName: String) extends ModelRequest

case class UnloadModelRequest(modelName: String) extends ModelRequest

case class CreateFrameStreamRequest(modelName: String,
                                    streamName: String,
                                    streamConfig: Option[StreamConfig] = None) extends ModelRequest

case class CreateFrameFlowRequest(modelName: String,
                                  streamName: String,
                                  format: String,
                                  flowConfig: Option[FlowConfig] = None) extends ModelRequest

case class GetFrameStreamRequest(modelName: String,
                                 streamName: String) extends ModelRequest

case class CreateRowStreamRequest(modelName: String,
                                  streamName: String,
                                  streamConfig: Option[StreamConfig] = None,
                                  spec: RowStreamSpec) extends ModelRequest

case class CreateRowFlowRequest(modelName: String,
                                streamName: String,
                                format: String,
                                flowConfig: Option[FlowConfig] = None,
                                inputSchema: StructType,
                                outputSchema: StructType) extends ModelRequest

case class GetRowStreamRequest(modelName: String,
                               streamName: String) extends ModelRequest

/** Select mode is either strict or relaxed.
  *
  * Strict select mode causes an error to be returned when
  *   fields are missing.
  *
  * Relaxed selects all fields possible and does not error
  *   when there are some missing.
  */
sealed trait SelectMode
object SelectMode {
  case object Strict extends SelectMode
  case object Relaxed extends SelectMode
}

object TransformOptions {
  import scala.language.implicitConversions

  def default: TransformOptions = TransformOptions()

  implicit def apply(o: Option[TransformOptions]): TransformOptions = o.getOrElse(TransformOptions.default)
}

/** Options that affect the result of a transform.
  *
  * @param select which fields to select
  * @param selectMode strict or relaxed select mode
  */
case class TransformOptions(select: Option[Seq[String]] = None,
                            selectMode: SelectMode = SelectMode.Strict)

class TransformError(message: String, backtrace: String) extends Exception {
  override def getMessage: String = message
}