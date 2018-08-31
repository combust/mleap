# MLeap Spring Boot

This module is a Spring Boot project that provides a HTTP interface to an MLeap executor.

To package, run

```sbt mleap-spring-boot/assemby```

Once built, you can run the following command, replacing the version

```java -jar mleap-spring-boot/target/scala-2.11/mleap-spring-boot-assembly-{VERSION}.jar```

to start the server. This will start the server on port 8080, by default.

The following endpoints are available:

1. POST /models : Loading a model, replacing the path to your model and the chosen model name

```
body='{"modelName":"{YOUR_MODEL_NAME}","uri":"file:{PATH_TO_BUNDLE_ZIP}","config":{"memoryTimeout":900000,"diskTimeout":900000},"force":false}'

curl --header "Content-Type: application/json" \
  --request POST \
  --data "$body" http://localhost:8080/models
```

2. DELETE /models/{MODEL_NAME} : Unloading a model, replacing the chosen model name

```
  curl --header "Content-Type: application/json" \
    --request DELETE \
    http://localhost:8080/models/{YOUR_MODEL_NAME}
```

3. GET /models/{MODEL_NAME} : Retrieving a loaded model, replacing the chosen model name

```
curl --header "Content-Type: application/json" \
    --request GET \
    http://localhost:8080/models/{YOUR_MODEL_NAME}
```

4. GET /models/{MODEL_NAME}/meta : Retrieving a loaded model meta information, replacing the chosen model name

```
  curl --header "Content-Type: application/json" \
    --request GET \
    http://localhost:8080/models/{YOUR_MODEL_NAME}/meta

```

5. POST /models/{MODEL_NAME}/transform: Transform or scoring request, replacing the chosen model name, format and encoded leap frame

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

Check out the available Swagger API documentation `mleap_serving_1.0.0_swagger.yaml` for more information or trying out the API.

See the README.md in `mleap-serving` about starting both a gRPC and HTTP server using a single MLeap executor.