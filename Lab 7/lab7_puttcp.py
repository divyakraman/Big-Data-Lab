import socket

TCP_IP='127.0.0.1'
host = socket.gethostname()
TCP_PORT=5002
BUFFER_SIZE=1024# Normally 1024, but we want fast response

s=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
s.bind(('',TCP_PORT))
s.listen(10)

conn,addr=s.accept()
print('Connection address:',addr)

while 1:
	data=conn.recv(BUFFER_SIZE)
	print 'Message received from PutTCP : ',data
conn.close()
