import datetime
import time
import sys

timenow=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print(timenow)
if len(sys.argv)>0:
	print(sys.argv)
with open('temp1.txt','a+') as f:
	f.write(''+timenow+'\n')
	f.write(timenow+'\n')
time.sleep(30)
