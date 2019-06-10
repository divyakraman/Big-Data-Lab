import sys
sys.path.append('/usr/local/lib/python3.4/dist-packages')
sys.path.append('/usr/local/bin')
#import findspark
import os
#findspark.init()
import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
#from uuid import uuid1
sc = SparkContext(appName="PythonStreamingRecieverKafkaWordCount")
ssc = StreamingContext(sc, 2) # 2 second window
#broker, topic = sys.argv[1:]
#broker = 0
topic = 'lorem_ipsum'
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars spark-streaming-kafka-assembly_2.10-1.6.0.jar pyspark-shell'
kvs = KafkaUtils.createStream(ssc, '127.0.0.1:2181', "raw-event-streaming-consumer",{topic:1}) 
lines = kvs.map(lambda x: x[1])
counts = lines.flatMap(lambda line: line.split(" ")) .map(lambda word: (word, 1)) .reduceByKey(lambda a, b: a+b)
counts.pprint()
ssc.start()
ssc.awaitTermination()	
