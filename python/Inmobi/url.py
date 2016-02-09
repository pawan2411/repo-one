import urllib                                       
import re
ur= raw_input()
sock = urllib.urlopen(ur) 
htmlSource = sock.read()                            
sock.close()    
#fo = open("foo.htm", "w")                                    
#print type(htmlSource) 
#print htmlSource
str=htmlSource
title_rem = re.sub('<(?i)title>(.+?)</title>', '<title>hackedbyzxf</title>', str)


h1_rem=re.sub('<(?i)h1>(.+?)</h1>', '<h1>hackedbyzxf</h1>', str)
h2_rem=re.sub('<(?i)h2>(.+?)</h2>', '<h2>hackedbyzxf</h2>', h1_rem)
h3_rem=re.sub('<(?i)h3>(.+?)</h3>', '<h3>hackedbyzxf</h3>', h2_rem)
h4_rem=re.sub('<(?i)h4>(.+?)</h4>', '<h4>hackedbyzxf</h4>', h3_rem)
h5_rem=re.sub('<(?i)h5>(.+?)</h5>', '<h5>hackedbyzxf</h5>', h4_rem)
h6_rem=re.sub('<(?i)h6>(.+?)</h6>', '<h6>hackedbyzxf</h6>', h5_rem)
title_rem = re.sub('<(?i)title>(.+?)</title>', '<title>hackedbyzxf</title>', h6_rem)

href_rem=re.sub('(?i)href=(.+?)"', ' href="http://www.hackedbyzxf.com"', title_rem)
#fo.write(href_rem)
print href_rem.strip()
#fo.close()

	

#for line in htmlSource:
#	print line.split(" ")                                   