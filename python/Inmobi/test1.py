len_q, rot= raw_input().split(" ")
#print rot
a=raw_input().split()
a = [long(x) for x in a]
#rot=11
for i in range(rot):
	#for j in range(len(a)):
	if a[i%long(len(a))]!=0:
		a[i%long(len(a))]=a[i%long(len(a))]-1
	else:
		k=i
		count=0
		flag=0
		while count!=len(a):
			if a[k%long(len(a))]!=0 and flag==0:
				a[k%long(len(a))]=a[k%long(len(a))]-1
				flag=1
			count=count+1
			#print a[k%len(a)]
			k=k+1
			
		
		#print a
		#print count
result=[]
for i in range(len(a)):
	if a[i]!=0:
		result.append(i+1)

if len(result)==0:
	print -1
else:
    for i in range(long(len(result))):
    	print result[i]


