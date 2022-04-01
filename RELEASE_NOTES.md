# Release 0.20.0-SNAPSHOT (NOT RELEASED YET)

### Breaking Changes

### New Features

### Bug Fixes
- Fixes 'cannot assign instance of java.lang.invoke.SerializedLambda' for Tensorflow transformer when using sparkTransform() method (#801)

### Improvements
- Upgrade to xgboost v1.5.2 (#799,#803)
- Upgrade to spark v3.2.0 (#799)
- Upgrade to tensorflow java 0.4.0 and tensorflow 2.7.1 (#802)

### Other Changes

# Release 0.19.0

### New Features
- `MapType` is now supported as a core data type (#789)
- `MapEntrySelector` is a new Transformer that can be used to select values from maps (#789)
- Spark Vectors can now be cast into MLeap Tensors (#791)

### Bug Fixes
- Fixes uid parity issues for certain Spark 3 transformers (#788)
- MLeap Tensor -> Spark Vector casting logic is fixed for non-increasing indices (#794)

### Improvements
- Upgrades to xgboost v1.3.1 (#778)
- Updates shading rules for databricks runtime assembly (#780)
- StringIndexerModel now performs faster lookups by caching the index size (#793)
- Upgrades springboot version to 2.6.2 and junit to 5.8.2

# Release 0.18.1

### Bug Fixes
- Fix (List <-> Tensor) casting when base types match
- Fix deserialization of legacy Spark 2 models

# Release 0.18.0

### Breaking Changes
- Scala 2.11 is no longer supported due to Spark 3 upgrade
- Please use 0.18.1 mleap version if you have models serialized with both Spark 2.x and Spark 3.x

### New Features
- MathBinaryModel now supports Logit operations
- TensorflowTransformerOp now supports serialization and deserialization using SavedModel format
- `Casting.cast` now supports conversions between ListType and TensorType

### Bug Fixes
- Fix OneHotEncoder Python serialization

### Improvements
- Upgrade to scikit-learn 0.22
- Upgrade to Spark 3.0.2
- Upgrade Tensorflow version to 2.4.1

### Other Changes

# Release 0.17.0

### Breaking Changes

### New Features
- upgrade to xgboost 1.0.0 - using h2oai Predictor
- support for using xgboost predictor when using xgboost regressor
- MathBinaryModel now supports Min and Max operations

### Bug Fixes
- fix Spark deserialization of random forest classifier to include numTrees

### Improvements
- scoring optimizations for Interacting and CountVectorizer

### Other Changes

# Release 0.16.1 - python only release

### Bug Fixes
- fix MathBinary serialization/deserialization in pyspark

# Release 0.16.0

### Breaking Changes
- Fix default ports when running grpc/http requests; default grpc port is 65328 and can be overridden via MLEAP_GRPC_PORT; default http port should be: 65327 and can be overridden via MLEAP_HTTP_PORT

### New Features
- Upgrade to Spark version 2.4.5
- Support for a performant implementation of the XGboost runtime (XGboost Predictor)
- Scikit-learn support for MultinomialLogisticRegression
- Support for min/max values other than defaults (i.e. 0.0 and 1.0) in MinMaxScalerModel
- Support for custom transformers (StringMap, MathUnary, MathBinary) in Pyspark
- Support MLWritable/MLReadable for custom transformers (StringMap, MathUnary, MathBinary) and fix this for Imputer transformer
- Fixes support for loading/storing bundles from/to hdfs in Pyspark
- Improve importing mleap __version__ for python modules

### Bug Fixes
- Fix XGBoost sparse vector support
- Fix MinMaxScalerModel outputs different in Spark vs MLeap
- Fix Spark deserialization for CountVectorizer transformer
- Added support for HandleInvalid in Bucketizer, VectorIndexer
- Fix setting HandleInvalid by default to OneHotEncoder for backwards compatibility
- Fixes MLReader for Imputer mleap implementation of Spark transformer

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

- The deprecated OneHotEncoder unfortunately had breaking changes in a few releases. In releases 0.11.0 and 0.12.0, the deserialization into MLeap was broken for OneHotEncoder. When using releases 0.13.0, 0.14.0, and 0.15.0, please ensure that the model returns the same results as before the upgrade, by potentially changing dropLast and handleInvalid values after deserialization. Alternatively, please use MLeap version 0.16.0 or higher, in case you have models serialized with other versions of MLeap that use OneHotEncoder. If your model uses OneHotEncoderEstimator or no one hot encoding, then you should not encounter any of the issues above. 
