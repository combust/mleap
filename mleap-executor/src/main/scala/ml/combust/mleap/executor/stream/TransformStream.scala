package ml.combust.mleap.executor.stream

import akka.NotUsed
import akka.stream.scaladsl.Flow
import ml.combust.mleap.executor.{TransformFrameRequest, TransformRowRequest}
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row, RowTransformer, Transformer}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/** Utilities for creating Akka stream flow stages
  */
object TransformStream {
  val DEFAULT_PARALLELISM: Int = 8

  /** Creates a flow for transforming leap frames.
    *
    * @param transformer transformer to use
    * @param parallelism how many transforms we can run in parallel
    * @param ec execution context
    * @tparam Tag companion object to match request with response
    * @return Akka stream flow for transforming leap frames
    */
  def frame[Tag](transformer: Transformer,
                 parallelism: Int = DEFAULT_PARALLELISM)
                (implicit ec: ExecutionContext): Flow[(TransformFrameRequest, Tag), (Try[DefaultLeapFrame], Tag), NotUsed] = {
    Flow[(TransformFrameRequest, Tag)].mapAsyncUnordered(parallelism) {
      case (request, tag) =>
        Future.fromTry(request.frame.map {
          frame =>
            transformer.transformAsync(frame).map {
              frame =>
                val tryFrame = request.options.select.map(select => frame.select(select: _*)).
                  getOrElse(Try(frame))
                (tryFrame, tag)
            }
        }).flatMap(identity)
    }
  }

  /** Creates a flow for transforming rows.
    *
    * @param rowTransformer transformer to use for each row
    * @param parallelism how many rows we can transform in parallel
    * @param ec execution context
    * @tparam Tag companion object to match request with response
    * @return Akka stream flow for transforming leap frames
    */
  def row[Tag](rowTransformer: RowTransformer,
               parallelism: Int = DEFAULT_PARALLELISM)
              (implicit ec: ExecutionContext): Flow[(TransformRowRequest, Tag), (Try[Option[Row]], Tag), NotUsed] = {
    Flow[(TransformRowRequest, Tag)].mapAsyncUnordered(parallelism) {
      case (request, tag) =>
        Future {
          val result = request.row.flatMap {
            row => Try(rowTransformer.transformOption(row))
          }

          (result, tag)
        }
    }
  }
}
