from kafka import KafkaConsumer, TopicPartition
import matplotlib.pyplot as plt
import numpy as np
import time

consumer1 = KafkaConsumer(bootstrap_servers='localhost:9092', consumer_timeout_ms=10000)
consumer1.subscribe(('virginicasetosa','versicolor'))
consumer2 = KafkaConsumer(bootstrap_servers='localhost:9092', consumer_timeout_ms=10000)
partition2 = TopicPartition('virginicasetosa',1)
consumer2.assign([partition2])
consumer3 = KafkaConsumer(bootstrap_servers='localhost:9092', consumer_timeout_ms=10000)
partition3 = TopicPartition('virginicasetosa',0)
consumer3.assign([partition3])
consumer3.seek_to_beginning(partition3)
consumer2.seek_to_beginning(partition2)

#print ("Topic\tOffset\tValue     \t\tPartition\tTimestamp")
print("Consumer 1 is subscribed to all topics and all partitions. Topic virginicasetosa partition 0 corresponds to class virginica, Topic virginicasetosa partition 1 corresponds to class setosa and class versicolor, both partitions correspond to class versicolor.")
print ("Topic\tValue(sepalLength,sepalWidth)\tPartition\t")
plt.figure()
for msg in consumer1:
    line = str(msg.topic) + "\t" 
    line = line + "\t" + str(msg.value)
    #line = str(msg.value)
    line = line + "\t in partition " + str(msg.partition)
    value = (msg.value)[1:-1].split(',')
    value = np.array(value)
    if(msg.topic=='virginicasetosa' and msg.partition==0):
	colour='r'
    if(msg.topic=='virginicasetosa' and msg.partition==1):
	colour='b'
    if(msg.topic=='versicolor'):
	colour='g'
    plt.scatter(value[0],value[1],c=colour)
    #plt.show()
    #line = line + "    \t\t" + str(msg.timestamp)
    print(line)

plt.show()

print("Consumer 2 corresponds to class setosa \t This is in partition 1 of topic virginicasetosa \n")
time.sleep(10)
print ("Topic\tValue(sepalLength,sepalWidth)\tPartition\t")
for msg in consumer2:
    line = str(msg.topic) + "\t" 
    line = line + "\t" + str(msg.value)
    #line = str(msg.value)
    line = line + "\t in partition " + str(msg.partition)
    #line = line + "    \t\t" + str(msg.timestamp)
    print(line)


print("Consumer 3 is subscribed to partition 0 of virginicasetosa which corresponds to class virginica. It prints out all data from start of partition 0 of virginicasetosa.")
time.sleep(10)
print ("Topic\tValue(sepalLength,sepalWidth)\tPartition\t")
for msg in consumer3:
    line = str(msg.topic) + "\t" 
    line = line + "\t" + str(msg.value)
    #line = str(msg.value)
    line = line + "\t in partition " + str(msg.partition)
    #line = line + "    \t\t" + str(msg.timestamp)
    print(line)

