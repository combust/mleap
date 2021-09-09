# Python integration package for MLeap

This package contains libraries to integrate MLeap with:
* PySpark
* Scikit-Learn
* TensorFlow (coming soon)

# Installation

```bash
$ pip install mleap
```

## PySpark Integration

MLeap's PySpark library provides serialization and deserialization functionality to/from Bundle.ML. There is 100% parity between MLeap's PySpark and Scala/Spark support and all of the supported transformers can be found [here](https://combust.github.io/mleap-docs/core-concepts/transformers/support.html).

We have both a [basic tutorial](https://combust.github.io/mleap-docs/py-spark/) and an [advance demo](https://github.com/combust/mleap-demo/blob/master/notebooks/PySpark%20-%20AirBnb.ipynb) of serializing and de-serializing using PySpark, but in short you can continue to write ML Pipelines as you normally would and we provide the following interface for serialization/de-serialization:

```python
# Define your pipeline
feature_pipeline = [string_indexer, feature_assembler]

featurePipeline = Pipeline(stages=feature_pipeline)

# Fit your pipeline
fittedPipeline = featurePipeline.fit(df)

# Serialize your pipeline
fittedPipeline.serializeToBundle("jar:file:/tmp/pyspark.example.zip", fittedPipeline.transform(df))
```

### StringMap transformer

```python
# dict of label mappings
labels = {'a': 1.0}

string_map_transformer = StringMap(
    labels, 'key_col', 'value_col', handleInvalid='keep', defaultValue=0.0)
```

### MathUnary transformer

Example usage:
```python
# dict of label mappings
input_dataframe = pd.DataFrame([[0.1, 0.2, 0.3]], columns=['f1'])

sin_transformer = MathUnary(
    operation=UnaryOperation.Sin,
    inputCol="f1",
    outputCol="sin(f1)",
)

sin_transformer.transform(input_dataframe)
```

## Scikit-Learn Integration

MLeap's Scikit-Learn library provides serialization (de-serialization coming soon) functionality to Bundle.ML. There is already parity between the math that Scikit and Spark transformers execute, and MLeap takes advantage of that to provide a common serialization format for the two technologies. 

A simple example is the `StandardScaler` transformer that normalizes the data given the mean and standard deviation. Both Spark and Scikit perform the standard normal transform on the data, and can both be serialized to the following format:

```json
{
    "op": "standard_scaler",
    "attributes": {
        "mean": {
              "double": [0.2354223, 1.34502332],
              "shape": {
                "dimensions": [{
                  "size": 2,
                  "name": ""
                }]
              },
             "type": "tensor"             
        },
        "std": {
              "double": [0.13842223, 0.78320249],
              "shape": {
                "dimensions": [{
                  "size": 2,
                  "name": ""
                }]
              },
             "type": "tensor"
        }
    }
}
```

Scikit-Learn pipelines, just like Spark Pipelines, can be serialized to an MLeap Bundle and deployed to an [MLeap runtime environment](https://combust.github.io/mleap-docs/mleap-runtime/).

You can also take your scikit pipelines and deploy them to your Spark cluster, because MLeap can de-serialize them into a Spark ML Pipeline and execute them on data frames.

## Documentation

Documentation can be found on our mleap docs page:
* [PySpark](https://combust.github.io/mleap-docs/getting-started/py-spark.html)
* [Scikit-Learn](https://combust.github.io/mleap-docs/getting-started/scikit-learn.html)

## Contributions
Contributions are welcome! Make sure all python tests pass.
You can run them from the top-level makefile:
```bash
make py37_test
```

If you'd rather use the inner `python/Makefile`, remember to source SCALA_CLASS_PATH by running:

```bash
source scripts/scala_classpath_for_python.sh
cd python/ && make test
```
