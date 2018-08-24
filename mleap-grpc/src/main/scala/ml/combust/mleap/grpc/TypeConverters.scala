package ml.combust.mleap.grpc

import java.net.URI

import ml.combust.bundle.dsl.BundleInfo
import ml.combust.mleap.executor
import ml.combust.mleap.executor.Parallelism
import ml.combust.mleap.pb._
import ml.combust.mleap.runtime.types.BundleTypeConverters._

import scala.concurrent.duration._

object TypeConverters {
  import scala.language.implicitConversions

  private def timeoutOption(duration: FiniteDuration): Option[FiniteDuration] = {
    if (duration.length > 0) { Some(duration) }
    else { None }
  }

  implicit def pbToMleapSelectMode(sm: SelectMode): executor.SelectMode = {
    if (sm.isSelectModeRelaxed) {
      executor.SelectMode.Relaxed
    } else if (sm.isSelectModeStrict) {
      executor.SelectMode.Strict
    } else { executor.SelectMode.Strict }
  }

  private implicit def pbToMleapSelect(select: Seq[String]): Option[Seq[String]] = {
    if (select.isEmpty) { None }
    else { Some(select) }
  }

  implicit def pbToMleapTransformOptions(options: Option[TransformOptions]): executor.TransformOptions = {
    options.map {
      options =>
        executor.TransformOptions(select = options.select, selectMode = options.selectMode)
    }
  }

  implicit def mleapToPbSelectMode(sm: executor.SelectMode): SelectMode = sm match {
    case executor.SelectMode.Strict => SelectMode.SELECT_MODE_STRICT
    case executor.SelectMode.Relaxed => SelectMode.SELECT_MODE_RELAXED
  }

  implicit def mleapToPbTransformOptions(options: executor.TransformOptions): TransformOptions = {
    TransformOptions(
      select = options.select.getOrElse(Seq()),
      selectMode = options.selectMode
    )
  }

  implicit def pbToMleapTransformOptions(options: TransformOptions): executor.TransformOptions = {
    executor.TransformOptions(
      select = options.select,
      selectMode = options.selectMode
    )
  }

  implicit def mleapToPbThrottleMode(mode: akka.stream.ThrottleMode): ThrottleMode = mode match {
    case akka.stream.ThrottleMode.Enforcing => ThrottleMode.THROTTLE_ENFORCING
    case akka.stream.ThrottleMode.Shaping => ThrottleMode.THROTTLE_SHAPING
    case _ => ThrottleMode.THROTTLE_SHAPING
  }

  implicit def pbToMleapThrottleMode(mode: ThrottleMode): akka.stream.ThrottleMode = mode match {
    case ThrottleMode.THROTTLE_SHAPING => akka.stream.ThrottleMode.Shaping
    case ThrottleMode.THROTTLE_ENFORCING => akka.stream.ThrottleMode.Enforcing
    case _ => akka.stream.ThrottleMode.Shaping
  }

  implicit def mleapToPbThrottle(throttle: executor.Throttle): Throttle = {
    Throttle(
      elements = throttle.elements,
      duration = throttle.duration.toMillis,
      maximumBurst = throttle.maxBurst,
      mode = throttle.mode
    )
  }

  implicit def pbToMleapThrottle(throttle: Throttle): executor.Throttle = {
    executor.Throttle(
      elements = throttle.elements,
      duration = throttle.duration.millis,
      maxBurst = throttle.maximumBurst,
      mode = throttle.mode
    )
  }

  implicit def mleapToPbBundleMeta(meta: executor.BundleMeta): BundleMeta = {
    BundleMeta(
      bundle = Some(meta.info.asBundle),
      inputSchema = Some(meta.inputSchema),
      outputSchema = Some(meta.outputSchema)
    )
  }

  implicit def pbToMleapBundleMeta(meta: BundleMeta): executor.BundleMeta = {
    executor.BundleMeta(
      info = BundleInfo.fromBundle(meta.bundle.get),
      inputSchema = meta.inputSchema.get,
      outputSchema = meta.outputSchema.get
    )
  }

  implicit def mleapToPbGetBundleMeta(request: executor.GetBundleMetaRequest): GetBundleMetaRequest = {
    GetBundleMetaRequest(modelName = request.modelName)
  }

  implicit def pbToMleapGetBundleMeta(request: GetBundleMetaRequest): executor.GetBundleMetaRequest = {
    executor.GetBundleMetaRequest(modelName = request.modelName)
  }

  implicit def mleapToPbStreamConfig(config: executor.StreamConfig): StreamConfig = {
    StreamConfig(
      idleTimeout = config.idleTimeout.map(_.toMillis),
      transformDelay = config.transformDelay.map(_.toMillis),
      parallelism = config.parallelism.map(_.get),
      throttle = config.throttle.map(mleapToPbThrottle),
      bufferSize = config.bufferSize
    )
  }

  implicit def pbToMleapStreamConfig(config: StreamConfig): executor.StreamConfig = {
    executor.StreamConfig(
      idleTimeout = config.idleTimeout.map(_.millis),
      transformDelay = config.transformDelay.map(_.millis),
      parallelism = config.parallelism.map(Parallelism(_)),
      throttle = config.throttle.map(pbToMleapThrottle),
      bufferSize = config.bufferSize
    )
  }

  implicit def mleapToPbModel(model: executor.Model): Model = {
    Model(name = model.name, uri = model.uri.toString, config = Some(model.config))
  }

  implicit def pbToMleapModel(model: Model): executor.Model = {
    executor.Model(name = model.name, uri = URI.create(model.uri), config = model.config.get)
  }

  implicit def mleapToPbRowStreamSpec(config: executor.RowStreamSpec): RowStreamSpec = {
    RowStreamSpec(
      schema = Some(config.schema),
      options = Some(config.options)
    )
  }

  implicit def pbToMleapRowStreamSpec(config: RowStreamSpec): executor.RowStreamSpec = {
    executor.RowStreamSpec(
      schema = config.schema.get,
      options = config.options.get
    )
  }

  implicit def mleapToPbModelConfig(config: executor.ModelConfig): ModelConfig = {
    ModelConfig(
      memoryTimeout = config.memoryTimeout.map(_.toMillis),
      diskTimeout = config.diskTimeout.map(_.toMillis)
    )
  }

  implicit def pbToMleapModelConfig(config: ModelConfig): executor.ModelConfig = {
    executor.ModelConfig(
      memoryTimeout = config.memoryTimeout.map(_.millis),
      diskTimeout = config.diskTimeout.map(_.millis)
    )
  }

  implicit def mleapToPbFlowConfig(config: executor.FlowConfig): FlowConfig = {
    FlowConfig(
      idleTimeout = config.idleTimeout.map(_.toMillis),
      transformDelay = config.transformDelay.map(_.toMillis),
      parallelism = config.parallelism.map(_.get),
      throttle = config.throttle.map(mleapToPbThrottle)
    )
  }

  implicit def pbToMleapFlowConfig(config: FlowConfig): executor.FlowConfig = {
    executor.FlowConfig(
      idleTimeout = config.idleTimeout.map(_.millis),
      transformDelay = config.transformDelay.map(_.millis),
      parallelism = config.parallelism.map(Parallelism(_)),
      throttle = config.throttle.map(pbToMleapThrottle)
    )
  }

  implicit def mleapToPbLoadModelRequest(request: executor.LoadModelRequest): LoadModelRequest = {
    LoadModelRequest(
      modelName = request.modelName,
      uri = request.uri.toString,
      config = request.config.map(mleapToPbModelConfig),
      force = request.force
    )
  }

  implicit def pbToMleapLoadModelRequest(request: LoadModelRequest): executor.LoadModelRequest = {
    executor.LoadModelRequest(
      modelName = request.modelName,
      uri = URI.create(request.uri),
      config = request.config.map(pbToMleapModelConfig),
      force = request.force
    )
  }

  implicit def mleapToPbUnloadModelRequest(request: executor.UnloadModelRequest): UnloadModelRequest = {
    UnloadModelRequest(modelName = request.modelName)
  }

  implicit def pbToMleapUnloadModelRequest(request: UnloadModelRequest): executor.UnloadModelRequest = {
    executor.UnloadModelRequest(modelName = request.modelName)
  }

  implicit def mleapToPbGetModelRequest(request: executor.GetModelRequest): GetModelRequest = {
    GetModelRequest(modelName = request.modelName)
  }

  implicit def pbToMleapGetModelRequest(request: GetModelRequest): executor.GetModelRequest = {
    executor.GetModelRequest(modelName = request.modelName)
  }

  implicit def mleapToPbCreateFrameStreamRequest(request: executor.CreateFrameStreamRequest): CreateFrameStreamRequest = {
    CreateFrameStreamRequest(
      modelName = request.modelName,
      streamName = request.streamName,
      streamConfig = request.streamConfig.map(mleapToPbStreamConfig)
    )
  }

  implicit def pbToMleapCreateFrameStreamRequest(request: CreateFrameStreamRequest): executor.CreateFrameStreamRequest = {
    executor.CreateFrameStreamRequest(
      modelName = request.modelName,
      streamName = request.streamName,
      streamConfig = request.streamConfig.map(pbToMleapStreamConfig)
    )
  }

  implicit def mleapToPbGetFrameStreamRequest(request: executor.GetFrameStreamRequest): GetFrameStreamRequest = {
    GetFrameStreamRequest(modelName = request.modelName, streamName = request.streamName)
  }

  implicit def pbToMleapGetFrameStreamRequest(request: GetFrameStreamRequest): executor.GetFrameStreamRequest = {
    executor.GetFrameStreamRequest(modelName = request.modelName, streamName = request.streamName)
  }

  implicit def mleapToPbCreateRowStreamRequest(request: executor.CreateRowStreamRequest): CreateRowStreamRequest = {
    CreateRowStreamRequest(
      modelName = request.modelName,
      streamName = request.streamName,
      streamConfig = request.streamConfig.map(mleapToPbStreamConfig),
      spec = Some(request.spec)
    )
  }

  implicit def pbToMleapCreateRowStreamRequest(request: CreateRowStreamRequest): executor.CreateRowStreamRequest = {
    executor.CreateRowStreamRequest(
      modelName = request.modelName,
      streamName = request.streamName,
      streamConfig = request.streamConfig.map(pbToMleapStreamConfig),
      spec = request.spec.get
    )
  }

  implicit def mleapToPbGetRowStreamRequest(request: executor.GetRowStreamRequest): GetRowStreamRequest = {
    GetRowStreamRequest(modelName = request.modelName, streamName = request.streamName)
  }

  implicit def pbToMleapGetRowStreamRequest(request: GetRowStreamRequest): executor.GetRowStreamRequest = {
    executor.GetRowStreamRequest(modelName = request.modelName, streamName = request.streamName)
  }

  implicit def mleapToPbRowStream(stream: executor.RowStream): RowStream = {
    RowStream(modelName = stream.modelName,
      streamName = stream.streamName,
      streamConfig = Some(stream.streamConfig),
      spec = Some(stream.spec),
      outputSchema = Some(stream.outputSchema))
  }

  implicit def pbToMleapRowStream(stream: RowStream): executor.RowStream = {
    executor.RowStream(modelName = stream.modelName,
      streamName = stream.streamName,
      streamConfig = stream.streamConfig.get,
      spec = stream.spec.get,
      outputSchema = stream.outputSchema.get)
  }

  implicit def mleapToPbFrameStream(stream: executor.FrameStream): FrameStream = {
    FrameStream(modelName = stream.modelName,
      streamName = stream.streamName,
      streamConfig = Some(stream.streamConfig))
  }

  implicit def pbToMleapFrameStream(stream: FrameStream): executor.FrameStream = {
    executor.FrameStream(modelName = stream.modelName,
      streamName = stream.streamName,
      streamConfig = stream.streamConfig.get)
  }
}
