import sys
from sklearn.linear_model import LogisticRegression
import pickle
import json
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
kafkaStream = KafkaUtils.createStream(ssc, '127.0.0.1:2181', "raw-event-streaming-consumer",{topic:1}) 
loaded_model = pickle.load(open(filename, 'rb'))
parsed = kafkaStream.map(lambda v: json.loads(v[1]))

Xdat = parsed

result = loaded_model.predict(Xdat)
print(result)
ssc.start()
ssc.awaitTermination()	
