strin=["eefg","effg","efgg"]
Matrix = [[0 for x in range(3)] for x in range(3)]


def rem_rep( str ):
	lstr= list(str)
	stc=[None]*(len(lstr))
	temp=None
	k=0
	for i in range(len(lstr)):
		if lstr[i]!=temp:
			stc[k]=lstr[i]
			k=k+1
		temp=lstr[i]
	#print stc
	return ''.join(stc[0:k])



for i in range(3):
	for j in range(3):
		if i==j:
			Matrix[i][j]=0
		else:
			if rem_rep(strin[i])==rem_rep(strin[j]):
				Matrix[i][j]= min(len(strin[i]),len(strin[j])) - len(rem_rep(strin[i]))


print Matrix