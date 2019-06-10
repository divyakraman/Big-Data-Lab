import pandas
import pickle
import time
import numpy as np
from sklearn.linear_model import LogisticRegression
from kafka import KafkaProducer
import json

url = "https://raw.githubusercontent.com/jbrownlee/Datasets/master/iris.csv"
filename = 'finalized_model.sav'
names = ['sepal-length', 'sepal-width', 'petal-length', 'petal-width', 'class']
dataset = pandas.read_csv(url, names=names)
cols = dataset.columns
Xdat = dataset.values[:, 0:4]

class NumpyEncoder(json.JSONEncoder):
	def default(self,obj):
		if isinstance(obj,np.ndarray):
			return obj.tolist()
		return json.JSONEncoder.default(self,obj)

loaded_model = pickle.load(open(filename, 'rb'))
result = loaded_model.predict(Xdat)
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v:json.dumps(v,cls=NumpyEncoder).encode('utf-8'))

for i, r in enumerate(result):
	time.sleep(0.4)
	pushdata=Xdat[i, 0:2]#r[0:2]
	if(r=='Iris-virginica'):
		producer.send('virginicasetosa',value=pushdata,partition=0)
	if(r=='Iris-setosa'):
		producer.send('virginicasetosa',value=pushdata,partition=1)
	if(r=='Iris-versicolor'):
		part = np.random.randint(0,2)
		producer.send('versicolor',value=pushdata,partition=part)
	print(r, Xdat[i, 0:2])
