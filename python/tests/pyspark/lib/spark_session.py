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

    # mleap pyspark wrappers have not been tested against scala 2.12. However,
    # they may still work as long as all jars are with the same scala version.
    classpath = os.environ['SCALA_CLASS_PATH']

    return builder \
        .config('spark.driver.extraClassPath', classpath) \
        .config('spark.executor.extraClassPath', classpath) \
        .getOrCreate()
