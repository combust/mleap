package ml.combust.mleap.executor.stream

import akka.NotUsed
import akka.stream.scaladsl.Flow
import ml.combust.mleap.executor._
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
                 parallelism: Parallelism): Flow[(StreamTransformFrameRequest, Tag), (Try[DefaultLeapFrame], Tag), NotUsed] = {
    Flow[(StreamTransformFrameRequest, Tag)].mapAsyncUnordered(parallelism) {
      case (request, tag) =>
        ExecuteTransform(transformer, request.frame, request.options).map {
          frame => (frame, tag)
        }
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
               parallelism: Parallelism): Flow[(StreamTransformRowRequest, Tag), (Try[Option[Row]], Tag), NotUsed] = {
    Flow[(StreamTransformRowRequest, Tag)].mapAsyncUnordered(parallelism) {
      case (request, tag) =>
        Future {
          val result = request.row.map(rowTransformer.transformOption)

          (result, tag)
        }
    }
  }
}
