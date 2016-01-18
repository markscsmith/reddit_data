__author__ = '-hax-'
import os
import sys
#os.environ['SPARK_HOME']='/usr/local/Cellar/apache-spark/1.5.1/libexec'
#sys.path.append('/usr/local/Cellar/apache-spark/1.5.1/libexec')


try:
    from pyspark import SparkContext
    from pyspark import SparkConf
    print ("Successfully imported Spark Modules")
except ImportError as e:
    print ("Can not import Spark Modules", e)
    sys.exit(1)
sc = SparkContext()
data = sc.textFile("/Users/mscs/Downloads/dnmarchives/")
data.map(lambda line: line)
data.take(1)
