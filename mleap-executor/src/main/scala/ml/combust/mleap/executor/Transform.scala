package ml.combust.mleap.executor

import java.util.UUID

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
    * @param request leap frame and transform options
    * @param ec execution context
    * @return a future of the transformed leap frame
    */
  def apply(transformer: Transformer,
            request: TransformFrameRequest)
           (implicit ec: ExecutionContext): Future[Try[DefaultLeapFrame]] = {
    Future.fromTry(request.frame.map {
      frame =>
      transformer.transformAsync(frame).flatMap {
        frame =>
          Future.fromTry {
            request.options.select.map {
              s =>
                request.options.selectMode match {
                  case SelectMode.Strict => frame.select(s: _*)
                  case SelectMode.Relaxed => Try(frame.relaxedSelect(s: _*))
                }
            }.getOrElse(Try(frame))
          }
      }
    }).flatMap(identity).map(Try(_)).recover {
      case err => Failure(err)
    }
  }
}

/** Specifies options for streams of transforms.
  *
  * @param initTimeout timeout for stream initialization
  * @param idleTimeout timeout for stream inactivity
  * @param transformTimeout timeout for transforming individual elements
  * @param parallelism parallelism of transforms
  * @param bufferSize size of buffer for transform elements
  */
case class StreamConfig(initTimeout: FiniteDuration,
                        idleTimeout: FiniteDuration,
                        transformTimeout: FiniteDuration,
                        parallelism: Parallelism,
                        bufferSize: Int)

/** Specifies the schema and transform options for
  * a row transformer.
  *
  * @param schema input schema of the rows
  * @param options transform options to apply for transform
  */
case class StreamRowSpec(schema: StructType,
                         options: TransformOptions = TransformOptions.default)

object TransformFrameRequest {
  import scala.language.implicitConversions

  implicit def apply(frame: Try[DefaultLeapFrame]): TransformFrameRequest = {
    TransformFrameRequest(frame, TransformOptions.default)
  }
}

/** Request to transform a leap frame.
  *
  * @param frame leap frame to transform
  * @param options transform options
  */
case class TransformFrameRequest(frame: Try[DefaultLeapFrame],
                                 options: TransformOptions = TransformOptions.default)

/** Request to transform a row using a row transformer.
  *
  * @param id id of the stream
  * @param row row to transform
  */
case class TransformRowRequest(id: UUID, row: Try[Row])

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