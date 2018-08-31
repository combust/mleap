# Bundle HDFS

This module provides and implementation of `BundleFileSystem` for a
Hadoop `FileSystem`. This allows saving/loading MLeap bundles directly
using HDFS.

This module is included as a dependency of `mleap-spark`.

If importing using `mleap-runtime`, this module will need to be included
as a separate dependency and possibly configured.

## Export Using Spark

When saving the bundle using Spark, a `HadoopBundleFileSystem` object
will automatically be generated and attached to the `SparkBundleContext`
when setting a sample DataFrame for export. You can disable automatically
registering the file system when passing in the data frame if you don't
desire this functionality.

```scala
import org.apache.spark.ml.bundle.SparkBundleContext
import org.apache.spark.ml.feature.StringIndexerModel
import org.apache.spark.sql.DataFrame
import ml.combust.mleap.spark.SparkSupport._
import java.net.URI

// Make sure to replace this with code to load your actual data
val dataset: DataFrame = ???
    
// Create a simple model
val stringIndexerModel = new StringIndexerModel(Array("label1", "label2"))

// Create an implicit SparkBundleContext for export
//
// NOTE: `registerHdfs = true` is not required as this is the default
implicit val sbc: SparkBundleContext = SparkBundleContext.
  defaultContext.withDataset(stringIndexerModel.transform(dataset),
  registerHdfs = true)
    
// Save the bundle using a URI
stringIndexerModel.writeBundle.save(new URI("hdfs:///tmp/test.bundle.zip"))
    
// Load the bundle using a URI
val loadedStringIndexerModel = new URI("hdfs:///tmp/test.bundle.zip").loadMleapBundle()
```

## Import Using MLeap Runtime

Make sure to include `bundle-hdfs` as a dependency of your project.
This will automatically register a default `HadoopBundleFileSystem`
with the default MLeap Bundle Registry. See `Custom Configuration`
for information on how to customize the underlying Hadoop file system.

```
import ml.combust.mleap.runtime.MleapSupport._
import java.net.URI

import ml.combust.mleap.core.feature.StringIndexerModel
import ml.combust.mleap.core.types.NodeShape
import ml.combust.mleap.runtime.transformer.feature.StringIndexer

// Create a simple transformer for export/import
val stringIndexer = StringIndexer(shape = NodeShape().
  withStandardInput("feature").
  withStandardOutput("feature_index"),
  model = StringIndexerModel(Seq("label1", "label2")))
    
// Use a URI to locate the bundle
val bundleUri = new URI("hdfs:///tmp/test.bundle.zip")
    
// Save the bundle using the HDFS file system
stringIndexer.writeBundle.save(bundleUri)
    
// Load the bundle
val loadedStringIndexer = bundleUri.loadMleapBundle()
```

## Custom Configuration

The underlying `import org.apache.hadoop.conf.Configuration`
can be configured if necessary. If using the `mleap-spark`
module, this shouldn't be necessary, as it will default to
using the configuration found in the `SparkContext`. This
may be necessary when trying to import a model using HDFS
and the `mleap-runtime`.

In order to customize the HDFS bundle file system, edit
your `application.conf` file in `src/main/resources` and
change the `ml.combust.mleap.hdfs.default` configuration.

```hocon
// Custom HDFS configuration
ml.combust.mleap.hdfs.default = {
  class = ml.bundle.hdfs.HadoopBundleFileSystem
  
  // Configure multiple schems to point to this bundle fs
  schemes = ["s3a", "hdfs"]
  
  // Custom hadoop configuration
  options {
  	"fs.defaultFS" = "hdfs://hello:12345"
  }
}
```

### Default reference.conf

This is the default `reference.conf` file that ships with the `bundle-hdfs` module.

```hocon
// Configure a default HDFS bundle file system
ml.combust.mleap.hdfs.default = {
  class = ml.bundle.hdfs.HadoopBundleFileSystem

  test {
    "hello.there" = "mang"
  }

  // Configure the registered schemes that point to this
  // Bundle file system. This is useful if you have the
  // HDFS and S3 file systems configured with Hadoop
  // And want to use both of them
  //
  // schemes = ["hdfs", "s3"]

  // This is used to configure the Hadoop Configuration
  // This is only needed if the default configuration
  // won't work for some reason
  //
  // Configuration options found here:
  //   https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/ClusterSetup.html
  //
  // options {
  //   "fs.defaultFS" = "hdfs://defaultfs:1234/"
  // }
}

// Register the default FS with the default bundle registry
ml.combust.mleap.registry.default.file-systems += ${ml.combust.mleap.hdfs.default}
```

## Configuring Programatically

```scala
import ml.bundle.hdfs.HadoopBundleFileSystem
import ml.combust.mleap.runtime.MleapContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

// Create the hadoop configuration
val config = new Configuration()
  
// Create the hadoop file system
val fs = FileSystem.get(config)
  
// Create the hadoop bundle file system
val bundleFs = HadoopBundleFileSystem(fs)
  
// Create an implicit custom mleap context for saving/loading
implicit val customMleapContext = MleapContext.defaultContext.copy(
registry = MleapContext.defaultContext.bundleRegistry.registerFileSystem(bundleFs)
)
```
