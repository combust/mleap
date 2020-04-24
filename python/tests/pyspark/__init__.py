import logging

# prevent py4j from flooding the output in case of errors
logging.getLogger("py4j").setLevel(logging.ERROR)
