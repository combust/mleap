import os
from pyspark.sql import SparkSession


def spark_session():
    """
    Minimize parallelism etc. to speed up execution.
    Test data is small & local, so just avoid all kind of overhead.
    """
    builder = SparkSession.builder \
        .config('spark.sql.shuffle.partitions', 1) \
        .config('spark.default.parallelism', 1) \
        .config('spark.shuffle.compress', False) \
        .config('spark.rdd.compress', False)

    # spark.jars doesn't seem to support classes folders. hence using the two extraClassPath properties.
    # difference is that spark.jars takes a comma-separated list of jars,
    #   while extraClassPath requires file paths with a platform-specific separator
    classpath = _mleap_classpath()
    builder = builder \
        .config('spark.driver.extraClassPath', classpath) \
        .config('spark.executor.extraClassPath', classpath)

    return builder \
        .enableHiveSupport() \
        .getOrCreate()


def _mleap_classpath():
    """
    Read classpath for mleap-spark-extension with the locally compiled classes.

    Classpath file can be refreshed by running `sbt mleap-spark-extension/compile`.
    However, that's only needed if making changes to dependencies.
    """
    classpath_file = os.path.join(
        os.path.dirname(__file__), '..', '..', '..', '..', 'mleap-spark-extension', 'target', 'classpath-runtime.txt')
    with open(classpath_file, 'r') as f:
        return f.read()
