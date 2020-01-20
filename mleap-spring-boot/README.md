# MLeap Spring Boot

This module is a Spring Boot project that provides a HTTP interface to an MLeap executor.

## Installation

MLeap Spring Boot is a Docker image hosted on [Docker Hub](https://hub.docker.com/r/combustml/mleap-spring-boot/).

To get started, pull the image to your local machine, replacing the version with the desired one.

```
docker pull combustml/mleap-spring-boot:{VERSION}
```

## Start Server

First let's start the Docker image. Make sure to mount a directory containing your models on the host
machine into the container. In this example, we will be storing our models in `/tmp/models` and mounting it in the container at `/models`.

```
mkdir /tmp/models
docker run -p 8080:8080 -v /tmp/models:/models combustml/mleap-spring-boot:{VERSION}
```

This will expose the model server locally on port `8080`.

## Available Endpoints

1. POST /models : Loading a model, replacing the name of your bundle zip ```{BUNDLE_ZIP}``` and the chosen model name ```{YOUR_MODEL_NAME}```

```
body='{"modelName":"{YOUR_MODEL_NAME}","uri":"file:/models/{BUNDLE_ZIP}","config":{"memoryTimeout":900000,"diskTimeout":900000},"force":false}'

curl --header "Content-Type: application/json" \
  --request POST \
  --data "$body" http://localhost:8080/models
```

2. DELETE /models/{YOUR_MODEL_NAME} : Unloading a model, replacing the chosen model name ```{YOUR_MODEL_NAME}```

```
  curl --header "Content-Type: application/json" \
    --request DELETE \
    http://localhost:8080/models/{YOUR_MODEL_NAME}
```

3. GET /models/{YOUR_MODEL_NAME} : Retrieving a loaded model, replacing the chosen model name ```{YOUR_MODEL_NAME}```

```
curl --header "Content-Type: application/json" \
    --request GET \
    http://localhost:8080/models/{YOUR_MODEL_NAME}
```

4. GET /models/{YOUR_MODEL_NAME}/meta : Retrieving a loaded model meta information, replacing the chosen model name ```{YOUR_MODEL_NAME}```

```
  curl --header "Content-Type: application/json" \
    --request GET \
    http://localhost:8080/models/{YOUR_MODEL_NAME}/meta

```

5. POST /models/transform: Transform or scoring request, replacing the chosen model name ```{YOUR_MODEL_NAME}```, format ```{YOUR_FORMAT}``` and the appropriately encoded leap frame ```{ENCODED_LEAP_FRAME}```

```
body='{"modelName":"{YOUR_MODEL_NAME}","format":"{YOUR_FORMAT}","initTimeout":"35000","tag":0,"frame":"{ENCODED_LEAP_FRAME}"}'

curl --header "Content-Type: application/json" \
  --request POST \
  --data "$body" http://localhost:8080/models/transform
```

Format can be either `ml.combust.mleap.binary` or `ml.combust.mleap.json` and your leap frame needs to be encoded with that format.

Note: The above endpoints are available either using:
- JSON (`Content-Type` header set to `application/json`)
- Protobuf (`Content-Type` header set to `application/x-protobuf`).

The previous scoring endpoints require you to encode the leap frame, either as JSON or protobuf. If you'd like to try out scoring without having to
encode the leap frame, you can use the following endpoint, replacing the chosen model name and your leap frame:

```
body='{YOUR_LEAP_FRAME_JSON}'

curl --header "Content-Type: application/json" \
  --header "timeout: 1000" \
  --request POST \
  --data "$body" http://localhost:8080/models/{YOUR_MODEL_NAME}/transform
```

Note: The above endpoint is available either using:
- JSON (`Content-Type` header set to `application/json`) with a JSON leap frame.
- Protobuf (`Content-Type` header set to `application/x-protobuf`) with a protobuf leap frame.

6. GET /actuator/* : exposes health and info endpoints for monitoring
```
curl --request GET http://localhost:8080/actuator
curl --request GET http://localhost:8080/actuator/health
curl --request GET http://localhost:8080/actuator/info
```

Check out the available Swagger API documentation `mleap_serving_1.0.0_swagger.yaml` for more information or trying out the API.

See the README.md in `mleap-serving` about starting both a gRPC and HTTP server using a single MLeap executor.

If you encounter an issue of

```
Unexpected HTTP/1.x request: POST /models 
```

you need to make sure curl or whichever method is used to send the requests uses HTTP2.

## Loading models at startup

This functionality can be activated by passing the configuration property `mleap.model.config`. Specifying a path to a single config will load that single config, while specifying a path to a directory will load all files from that directory.

### Example Docker workflow

The project directory is assumed to look like this:

```
├── Dockerfile
├── config
│   ├── application.properties
│   └── startup
│       ├── airbnb-lr.json
│       └── airbnb-rf.json
└── models
    ├── airbnb.model.lr.zip
    └── airbnb.model.rf.zip
```

#### config/application.properties

```
mleap.model.config=/opt/docker/config/startup
```

#### Dockerfile

```Dockerfile
FROM combustml/mleap-spring-boot

# Override default server port if needed
ENV SERVER_PORT=8080
EXPOSE 8080

# Copy models
COPY models/*.zip models/

# Copy application.properties
COPY config/application.properties ./application.properties

# Copy startup configs
COPY config/startup/*.json ./config/startup/
```

#### config/startup/airbnb-lr.json

```json
{
    "config": {
        "diskTimeout": 525600000,
        "memoryTimeout": 525600000
    },
    "modelName": "airbnb-lr",
    "uri": "file:/opt/docker/models/airbnb.model.lr.zip"
}
```

#### config/startup/airbnb-lr.json

```json
{
    "config": {
        "diskTimeout": 525600000,
        "memoryTimeout": 525600000
    },
    "modelName": "airbnb-rf",
    "uri": "file:/opt/docker/models/airbnb.model.rf.zip"
}
```

After building the docker image and starting the container, you can verify both models are loaded by querying their respective `/meta` endpoints.
