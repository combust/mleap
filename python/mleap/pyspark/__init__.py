import mleap.pyspark.feature
import sys

sys.modules['pyspark.ml.mleap'] = mleap
sys.modules['pyspark.ml.mleap.pyspark'] = sys.modules['mleap.pyspark']
sys.modules['pyspark.ml.mleap.feature'] = sys.modules['mleap.pyspark.feature']
