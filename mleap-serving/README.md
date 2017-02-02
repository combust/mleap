# MLeap Serving

MLeap serving provides a lightweight docker image for setting up RESTful
API services with MLeap models. It is meant to be very simple and used
as a tool to get prototypes up and running quickly.

## Installation

MLeap serving is a Docker image hosted on [Docker Hub](https://hub.docker.com/r/combustml/mleap-serving/).

To get started, pull the image to your local machine:

```
docker pull combustml/mleap-serving:0.6.0-SNAPSHOT
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
docker run -v /tmp/models:/models combustml/mleap-serving:0.6.0-SNAPSHOT
```

This will expose the model server locally on port `65327`.

### Load Model

Use curl to load the model into memory. If you don't have your own
model, download one of our example models. Make sure to place it in the
models directory you mounted when starting the server.

1. [AirBnB Linear Regression](https://s3-us-west-2.amazonaws.com/mleap-demo/airbnb.model.lr-0.6.0-SNAPSHOT.zip)
2. [AirBnB Random Forest](https://s3-us-west-2.amazonaws.com/mleap-demo/airbnb.model.rf-0.6.0-SNAPSHOT.zip)

```
curl -XPUT -H "content-type: application/json" \
  -d '{"path":"/models/<my model>.zip"}' \
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
