import os, sys
def configure_spark(spark_home=None, pyspark_python=None):
    spark_home = spark_home or "/usr/local/Cellar/apache-spark/1.5.1/libexec"
    os.environ['SPARK_HOME'] = spark_home

    # Add the PySpark directories to the Python path:
    sys.path.insert(1, os.path.join(spark_home, 'python'))
    sys.path.insert(1, os.path.join(spark_home, 'python', 'pyspark'))
    sys.path.insert(1, os.path.join(spark_home, 'python', 'build'))
    sys.path.insert(1, os.path.join(spark_home, 'python', 'lib/py4j-0.8.2.1-src.zip'))

    # If PySpark isn't specified, use currently running Python binary:
    pyspark_python = pyspark_python or sys.executable
    os.environ['PYSPARK_PYTHON'] = pyspark_python

configure_spark()
from pyspark import SparkContext
from pyspark import SparkConf

sc = SparkContext()
data = sc.textFile("/Users/mscs/Downloads/dnmarchives/")
test = data.take(100)