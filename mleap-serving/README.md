# MLeap Serving

MLeap serving provides a lightweight docker image for setting up RESTful
API services with MLeap models. It is meant to be very simple and used
as a tool to get prototypes up and running quickly.

## Installation

MLeap serving is a Docker image hosted on [Docker Hub](https://hub.docker.com/r/combustml/mleap-serving/).

To get started, pull the image to your local machine:

```
docker pull combustml/mleap-serving:0.9.0-SNAPSHOT
```

## Usage

In order to start using your models as a REST API, we will need to:

1. Start the server in Docker
2. Load our model into memory
3. Transform a leap frame

### Start Server

First let's start the Docker image so we can start transforming data.
Make sure to mount a directory containing your models on the host
machine into the container. In this example, we will be storing our
models in `/tmp/models` and mounting it in the container at `/models`.

```
mkdir /tmp/models
docker run -p 65327:65327 -v /tmp/models:/models combustml/mleap-serving:0.9.0-SNAPSHOT
```

This will expose the model server locally on port `65327`.

NOTE: Outside of Docker, you can utilize the `MLEAP_SERVER_HOSTNAME`
and the `MLEAP_SERVER_PORT` environment variables to explicitly control
where MLeap serving listens. The below example will bind the RESTful
web interface to the 127.0.0.1 interface on TCP port 12345, rather than
the default, all interfaces (0.0.0.0) on TCP port 65327.

```
export MLEAP_SERVER_HOSTNAME=127.0.0.1
export MLEAP_SERVER_PORT=12345
/path/to/mleap-serving # update this with the real path to the mleap-serving binary
```

### Load Model

Use curl to load the model into memory. If you don't have your own
model, download one of our example models. Make sure to place it in the
models directory you mounted when starting the server.

1. [AirBnB Linear Regression](https://github.com/combust/mleap/raw/master/mleap-benchmark/src/main/resources/models/airbnb.model.lr.zip)
2. [AirBnB Random Forest](https://github.com/combust/mleap/raw/master/mleap-benchmark/src/main/resources/models/airbnb.model.rf.zip)

```
curl -XPUT -H "content-type: application/json" \
  -d '{"path":"/models/<my model>.zip"}' \
  http://localhost:65327/model
```

### Retrieve Model Schema

```
curl -XGET -H "content-type: application/json" \
  http://localhost:65327/model
```

### Transform

Next we will use our model to transform a JSON-encoded leap frame. If
you are using our AirBnB example models, you can download the leap frame
here:

1. [AirBnB Leap Frame](https://s3-us-west-2.amazonaws.com/mleap-demo/frame.airbnb.json)

Save the frame to `/tmp/frame.airbnb.json` and then let's transform it
using our server.

```
curl -XPOST -H "accept: application/json" \
  -H "content-type: application/json" \
  -d @/tmp/frame.airbnb.json \
  http://localhost:65327/transform
```

You should get back a result leap frame, as JSON, that you can then
extract the result from. If you used one of our example AirBnB models,
the last field in the leap frame will be the prediction.

### Unload Model

If for some reason you don't want any model to be loaded into memory,
but keep the server running, just DELETE the `model` resource:

```
curl -XDELETE http://localhost:65327/model
```
