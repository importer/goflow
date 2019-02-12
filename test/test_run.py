import datetime

timenow=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print(timenow)
with open('temp.txt','a+') as f:
	f.write(timenow+'\n')