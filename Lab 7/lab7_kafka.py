from kafka import KafkaConsumer, TopicPartition
import matplotlib.pyplot as plt
import numpy as np
import time
from kafka import KafkaProducer
import pickle
import json

# consumer 1
consumer_1=KafkaConsumer(bootstrap_servers='localhost:9092')
consumer_1.subscribe(('virginicasetosa'))
filename='finalized_model.sav'
loaded_model=pickle.load(open(filename, 'rb'))
# class NumpyEncoder(json.JSONEncoder):
# 	def default(self,obj):
# 		if isinstance(obj,np.ndarray):
# 			return obj.tolist()
# 		return json.JSONEncoder.default(self,obj)

producer=KafkaProducer(bootstrap_servers='localhost:9092')
# value_serializer=lambda x : json.dumps(x,cls=NumpyEncoder).encode('utf-8'))

print('Before receiving..')
for message in consumer_1:
	 print 'Received message from PublishKafka is : ',message.value.rstrip('\n')
	 m1=message.value
	 m1=m1.rstrip('\n')
	 m=[float(x) for x in m1.split(',')]
	 value=np.array((m))
	 value=np.reshape(value,(1,-1))
	 result=loaded_model.predict(value)
	 print 'Message sent to ConsumerKafka : ',result[0]
	 print ''
	 producer.send('versicolor',value=m1+' '+str(result[0]))
	 time.sleep(1)
