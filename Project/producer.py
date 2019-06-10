from kafka import KafkaProducer
import time
import numpy as np
import json

class NumpyEncoder(json.JSONEncoder):
	def default(self,obj):
		if isinstance(obj,np.ndarray):
			return obj.tolist()
		return json.JSONEncoder.default(self,obj)


producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v:json.dumps(v,cls=NumpyEncoder).encode('utf-8'))
dataset = pandas.read_csv('testset.csv', names=names)

i = len(dataset)

for j in range(i):
	producer.send('lorem_ipsum',value=dataset[j])


