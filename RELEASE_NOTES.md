# Release 0.16.0 (pending, not released yet)

### Breaking Changes
- Fix default ports when running grpc/http requests; default grpc port is 65328 and can be overridden via MLEAP_GRPC_PORT; default http port should be: 65327 and can be overridden via MLEAP_HTTP_PORT

### New Features
- Scikit-learn support for MultinomialLogisticRegression
- Support for min/max values other than defaults (i.e. 0.0 and 1.0) in MinMaxScalerModel
- Support for custom transformers (StringMap, MathUnary, MathBinary) in Pyspark
- Upgrade to Spark version 2.4.5

### Bug Fixes
- Fix XGBoost sparse vector support
- Fix MinMaxScalerModel outputs different in Spark vs MLeap
- Fix Spark deserialization for CountVectorizer transformer
- Fix adding support for HandleInvalid.Error in Bucketizer
- Fix setting HandleInvalid.Error by default to OneHotEncoder for backwards compatibility

### Improvements
- Minor documentation updates

### Other Changes

# Release 0.15.0

### Breaking Changes
- None

### New Features
- Load models at start up in mleap-spring-boot
- Add support for Python 3
- StringMap transformer - add new optional parameters handleInvalid & defaultValue
- Add support for LinearSVC transformer/model 

### Bug Fixes
- Fix Tensorflow bundle writing when transform() method isn't necessarily called 
- Fix FrameReader reading a very large mleap frame

### Improvements
- Update xgboost4j and fix databricks runtime
- Use openjdk:8-jre-slim as docker base image
- Bump urllib3 from 1.23 to 1.24.2 in python package
- Add default grpc port to docker config
- General documentation improvements


# Older releases

We make every effort for the serialization format to be backwards compatible between different versions of MLeap. Please note below some important notes regarding backwards compatibility. 

- OneHotEncoder/OneHotEncoderEstimator unfortunately had breaking changes in a few releases (0.11, 0.12, 0.14, 0.15), if you need older Spark versions, please use MLeap version 0.10.3, 0.13 or else please use MLeap version 0.16.0 or higher.