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

### Other Changes
- None

# Release 0.16.0 (pending, not released yet)

### Breaking Changes
- None

### New Features
- Scikit-learn support for Multinomial Logistic Regression
- Support for min/max values other than defaults (i.e. 0.0 and 1.0) in MinMaxScalerModel

### Bug Fixes
- Fix XGBoost sparse vector support
- Fix MinMaxScalerModel outputs different in Spark vs MLeap
- Fix Spark deserialization for CountVectorizer transformer

### Improvements
- Minor documentation updates

### Other Changes
