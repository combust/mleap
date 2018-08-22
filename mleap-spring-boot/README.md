# MLeap Spring Boot

This module is a Spring Boot project that provides a HTTP interface to an MLeap executor.

Once built, you can run 

```java -jar mleap-spring-boot.jar```

to start the server. This will start the server on port 8080, by default.

To change the port, for example to 9000, you can run

```java -jar mleap-spring-boot.jar --server.port=9000```

The following endpoints are available:

1. POST /models : Loading a model, replacing the path to your model and the chosen model name

TODO add example

2. DELETE /models/{MODEL_NAME} : Unloading a model, replacing the chosen model name

TODO add example

3. GET /models/{MODEL_NAME} : Retrieving a loaded model, replacing the chosen model name

TODO add example

4. GET /models/{MODEL_NAME}/meta : Retrieving a loaded model meta information, replacing the chosen model name

TODO add example

5. POST /models/{MODEL_NAME}/transform: Transform or scoring request, replacing the chosen model name

TODO add example

These endpoints are available either using: 
- JSON (`Content-Type` header set to `application/json `)
- Protobuf (`Content-Type` header set to `application/x-protobuf`).

Check out the available Swagger API documentation `mleap_serving_1.0.0_swagger.yaml` for more information or trying out the API.

See the README.md file in `mleap-serving` about starting both a gRPC and HTTP server using a single MLeap executor.