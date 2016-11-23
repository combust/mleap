import mleap.pyspark.feature
import pyspark.ml
import sys

sys.modules['pyspark.ml'].mleap.pyspark = mleap.pyspark
sys.modules['pyspark.ml.mleap'] = mleap
sys.modules['pyspark.ml.mleap.pyspark'] = sys.modules['mleap.pyspark']
sys.modules['pyspark.ml.mleap.pyspark.feature'] = sys.modules['mleap.pyspark.feature']
