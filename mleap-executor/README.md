# MLeap Executor

The MLeap Executor handles the following tasks:

1. Loading bundles from the local file system and HTTP sources (as well as other sources by implementing the `Repository` trait.
2. Loading models into memory for execution and unloading them when not being used.
3. Single transform requests of leap frames.
4. Streaming frame transforms with support for transform delays, throttling, idle timeouts, parallelism and buffering.
5. Streaming row transforms for best performance. Includes support for transform delays, throttling, idle timeouts, parallelism and buffering.

This project is meant to support the `mleap-spring-boot` and `mleap-grpc-server` modules as the backend engine for serving requests. In the future, it will be used to support Kafka streaming applications, a CLI tool and other MLeap integrations.

## Repository

A repository is a source for downloading MLeap bundles. The currently supported repositories are:

1. The local file system
2. HTTP downloads
3. S3 file system (early support, untested, see `mleap-repository-s3`)

By default, the executor makes the local file system and HTTP downloads available as a source of models.

## TransformService

A transform service is a trait that implements all functionality required to load, unload, transform and stream data for the purpose of transforming.

There are three supported TransformService implementation for open-source MLeap:

1. `MleapExecutor` - provided by this module, this is an ActorSystem extension and sets up a `LocalTransformService` under the hood to execute MLeap models.
2. `LocalTransformService` - provided by this module, executes models using Akka actors and Akka streams on the local machine.
3. `GrpcClient` - provided by the `mleap-grpc` module, executed models using a gRPC server that implements the MLeap RPC service.

## Instructions

Here are usage instructions for using a `TransformService`. The easiest way to get up and running with a `TransformSerivce` is to use the `MleapExecutor` Actor System extension.

### Initializing MleapExecutor

```scala
import akka.actor.ActorSystem
import ml.combust.mleap.executor.MleapExecutor

// Create an Akka actor system for the executor
implicit val actorSystem: ActorSystem = ActorSystem("MleapExecutor")

// Initialize the MleapExecutor extension
val executor: MleapExecutor = MleapExecutor(actorSystem)
```

### Loading a Model

Find an example model at `mleap-executor-testkit/src/main/resources/models/airbnb.model.rf.zip`.

```scala
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

// Load the model with a name and path
val model = Await.result(transformService.loadModel(LoadModelRequest(

  // A name for the model, can be anything.
  // This is used to identity the model in subsequent
  //   transform, stream and unload calls.
  modelName = "airbnb_rf",
  
  // The URI can be either a local file or an HTTP file
  uri = "file:///absolute/path/to/model.zip",
  
  // An optional config for loading the model
  config = Some(ModelConfig(
  
  	 // This is how long the model will stay idle in memory
  	 //   (no transforms for 15 minutes and no attached streams).
  	 // After 15 minutes of inactivity, the model will unload
  	 // itself from memory until another request is made, when
  	 // it will load itself back into memory on demand.
    memoryTimeout = Some(15.minutes),
    
    // (Not implemented yet) this is how long the model will
    //   be cached on disk, in case the model is unloaded
    //   from the executor and then is requested to be loaded again.
    diskTimeout = Some(15.minutes)
  ))
))(10.seconds), 10.seconds)
```

### Unloading a Model

```scala
// Unload a model from memory. This will stop all running streams.
Await.result(transformService.unloadModel(UnloadModelRequest(

  // Name of the model to unload. This is set when loading the model.
  modelName = "airbnb_rf"
))(10.seconds)
```

### Transforming LeapFrames

Find an example leap frame at `mleap-executor-testkit/src/main/resources/leap_frame/frame.airbnb.json`.

```scala
import ml.combust.mleap.runtime.serialization.FrameReader

// Load the leap frame from a file
val frameFile = new File("/absolute/path/to/frame.json")
val frame = FrameReader().read(frameFile).get

// Transform the leap frame using the airbnb model we loaded earlier
val transformedFrame = Await.result(
  executor.transform(TransformFrameRequest("airbnb_rf", frame))
)(1.second)

// Show the results
transformedFrame.show()
```

### Creating a Row Stream

When speed is essential and embedding MLeap is not an option, using row streams is the best option. Row-based transforming will:

1. Get rid of serialization overhead of the leap frame schema
2. Get rid of the overhead of calculating the output schema on every transform

```scala
// Instantiate a stream for the sample model.
// You don't transform directly using a stream, instead you
// attach a row flow to the stream when you are ready to transform.
//
// This gives a lot of control over streams. You can not only control
// the idle timeout, transform delay, throttle and parallelism of the
// stream. You can also control the same variables for any flow attached
// to the stream.

val rowStream = Await.result(transformService.createRowStream(CreateRowStreamRequest(
  // Name of the model to build the stream for
  modelName = "airbnb_rf",
  
  // Name of the stream
  streamName = "stream1",
  
  // Optional stream config, will fall back
  // to defaults provided in the configuration
  // if available
  streamConfig = Some(StreamConfig(
  
    // This stream will destroy itself if it is
    // idle for this length of time
    idleTimeout = Some(1.minute),
    
    // Delays each request by this duration
    transformDelay = Some(10.millis),
    
    // Throttles requests using these parameters
    // See documentation on the Akka stream
    // throttle stage for more information
    throttle = Some(Throttle(
    	elements = 10,
    	maxBurst = 100,
    	duration = 1.second,
    	model = ThrottleMode.shaping
    )),
    
    // Maximum number of requests to be handled
    // concurrently
    parallelism = Some(8),
    
    // Number of requests to store in memory
    // before backpressuring
    bufferSize = Some(1024)
  )),
  
  // The stream specification defines the input
  // schema and the serialization format of the data
  spec = StreamSpec(
  
    // Input schema of the data
    schema = frame.schema,
    
    // Transform options, specify which
    // Output fields you are interested in
    options = TransformOptions(
    
       // Which output fields to select
    	select = Some(Seq("prediction")),
    	
    	// Relaxed mode won't throw errors
    	// When selecting fields that don't exist
    	selectMode = SelectMode.Relaxed
    )
  )
))(10.seconds), 10.seconds)
```

### Transforming Rows with a Row Flow

A Row Flow attaches to a Row Stream in order to start executing transforms. The idle timeout, throttle, transform delay, and parallelism of the flow are all limited by the underlying stream. A row flow is represented by an Akka `Flow[(StreamTransformRowRequest, Tag), (Try[Option[Row]], Tag)]`. The Tag is an arbitrary object used to associate the request with the response, as they can be returns out of order.

```scala
val flow = executor.createRowFlow[Int](
	CreateRowFlowRequest(
	
	  // Name of the model with the stream
	  modelName = "airbnb_rf",
	  
	  // Name of the stream attached to the model
	  streamName = "stream1",
	  
	  // Serialization format used to transmit data
	  // This is ignored for local executors
	  format = BuiltinFormats.binary,
	  
	  // Optional configuration for the flow
	  flowConfig = Some(FlowConfig(
	  	idleTimeout = None,
    	transformDelay = None,
    	parallelism = Some(4),
    	throttle = None
	  )),
	  
	  // Input/output schema must match what comes
	  // back when creating the row stream
	  inputSchema = rowStream.spec.schema,
	  outputSchema = rowStream.outputSchema))(10.seconds)
	  
val rowSource = Source.single((StreamTransformRowRequest(Try(frame.dataset.head)), 23))
val rowsSink = Sink.head[(Try[Option[Row]], Int)]

// Use the flow in a materialized Akka stream
val result = rowSource.via(flow).to(rowsSink).run()
```

## Testing

The `mleap-executor-testkit` module provides a useful trait for testing implementations of `TransformService`. Simply mix the `TransformServiceSpec` trait into your spec and implement the required methods. This will run a full set of tests against the executor to make sure it works as expected.

## Configuration

The `MleapExecutor` extension can be configured using Typesafe Configuration files. Here is the `reference.conf` file that is included in `mleap-executor`.

```hocon
ml.combust.mleap.executor {
  // Configure the default repository, which supports both local files
  // and HTTP downloads.
  repository {
    class = "ml.combust.mleap.executor.repository.MultiRepositoryProvider$"

    repositories = [{
      class = "ml.combust.mleap.executor.repository.FileRepositoryProvider$"
    }, {
      class = "ml.combust.mleap.executor.repository.HttpRepositoryProvider$"
    }]
  }

  // Defaults for timeouts, streams and flows
  default {
    // Default amount of time models will stick around in memory idle
    // before unloading
    default-memory-timeout = 15 minutes

    // Default amount of time to keep a model on disk before deleting
    // the bundle to reclaim space
    default-disk-timeout = 15 minutes

    // Default parameters for transform streams
    stream {
      default-parallelism = 1
      default-buffer-size = 1024

      // Optionally specify a throttle that will be applied
      // to all models
      //
      // default-throttle = {
      //   elements = 100
      //   max-burst = 200
      //   duration = 10 seconds
      //   mode = shaping
      // }

      // Optionally specify an idle timeout for all models
      // default-idle-timeout = 1 minute

      // Optionally specify a transform delay for all models
      // default-transform-delay = 10 millis
    }

    // Default parameters for flows
    flow {
      default-parallelism = 1

      // Optionally specify a throttle that will be applied
      // to all models
      //
      // default-throttle = {
      //   elements = 100
      //   max-burst = 200
      //   duration = 10 seconds
      //   mode = shaping
      // }

      // Optionally specify an idle timeout for all models
      // default-idle-timeout = 1 minute

      // Optionally specify a transform delay for all models
      // default-transform-delay = 10 millis
    }
  }

  // Default configuration options for file/http repositories
  repository-defaults {
    file {
      
      // Set this to true if the file should be moved to temporary storage
      // instead of copied from the source location
      move = false
      
      // Number of threads to perform operations with
      threads = 4
    }

    http {
      
      // Number of threads to perform operations with
      threads = 4
    }
  }
}
```