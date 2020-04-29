import os
import pyspark
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
    return builder \
        .config('spark.driver.extraClassPath', classpath) \
        .config('spark.executor.extraClassPath', classpath) \
        .getOrCreate()


def _mleap_classpath():
    """
    Read classpath for mleap-spark-extension with the locally compiled classes.

    Note that pyspark 2.4.4 comes with spark jars with specific scala version, for example:

        mleap/python/venv/lib/python3.7/site-packages/pyspark/jars/spark-core_2.11-2.4.4.jar

    Thus, pyspark is incompatible with scala 2.12 and only works with scala 2.11.

    This means that mleap pyspark wrappers have not been tested against scala 2.12. However, they may still work as long
    as all jars are with the same scala version.

    Classpath file can be refreshed manually by running `sbt mleap-spark-extension/writeRuntimeClasspathToFile`.
    However, that's only needed if making changes to dependencies, and any sbt +compile runs writeRuntimeClasspathToFile
    """
    classpath_file_dir = os.path.join(os.path.dirname(__file__), '..', '..', '..', '..',
                                      'mleap-spark-extension', 'target')
    classpath_file = list(filter(lambda x: x.startswith('classpath-runtime_'), os.listdir(classpath_file_dir)))[0]
    with open(os.path.join(classpath_file_dir, classpath_file), 'r') as f:
        return f.read()
