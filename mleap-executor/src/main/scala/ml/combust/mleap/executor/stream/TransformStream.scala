package ml.combust.mleap.executor.stream

import akka.NotUsed
import akka.stream.scaladsl.Flow
import ml.combust.mleap.executor.{Parallelism, TransformFrameRequest, TransformRowRequest}
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Row, RowTransformer, Transformer}

import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/** Utilities for creating Akka stream flow stages
  */
object TransformStream {
  /** Creates a flow for transforming leap frames.
    *
    * @param transformer transformer to use
    * @param ec execution context
    * @param parallelism how many transforms we can run in parallel
    * @tparam Tag companion object to match request with response
    * @return Akka stream flow for transforming leap frames
    */
  def frame[Tag](transformer: Transformer)
                (implicit ec: ExecutionContext,
                 parallelism: Parallelism): Flow[(TransformFrameRequest, Tag), (Try[DefaultLeapFrame], Tag), NotUsed] = {
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
    * @param ec execution context
    * @param parallelism how many rows we can transform in parallel
    * @tparam Tag companion object to match request with response
    * @return Akka stream flow for transforming leap frames
    */
  def row[Tag](rowTransformer: RowTransformer)
              (implicit ec: ExecutionContext,
               parallelism: Parallelism): Flow[(TransformRowRequest, Tag), (Try[Option[Row]], Tag), NotUsed] = {
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
