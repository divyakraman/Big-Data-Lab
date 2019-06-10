import time
import socket
import pandas as pd
import numpy as np
import SocketServer

TCP_IP='127.0.0.1'
host = socket.gethostname()
TCP_PORT=9081
BUFFER_SIZE=1024# Normally 1024, but we want fast response

s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
#SocketServer.TCPServer.allow_reuse_address = True
# s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)

#s.connect(('',TCP_PORT))
s.bind(('',TCP_PORT))
s.listen(10)

conn,addr=s.accept()
print('Connection address:',addr)

df=pd.read_csv('iris.csv')
data=df.values
print(df.head())

for d in data : 
	d=d[:-1]
	d1=[str(x) for x in d]
	d1=','.join(d1)
	d1=d1+'\n'
	# d1=str(d)+'\n'
	print 'Message sent to GetTCP : ',d1
	d2=bytearray(d1,'utf-8')
	conn.send(d2)
	time.sleep(1)
conn.close()
