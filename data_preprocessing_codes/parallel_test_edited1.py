import os
from sets import Set
import csv
from parallel_framework import ParallelRunner
from collections import OrderedDict
#number of parallel threads
dist_count = 2

folders = [[] for i in range(dist_count)]

#distributing folders
def folderDistributor(filepath):
   print(filepath)
   for directory,sublist,flist in os.walk(filepath):
     count = 0
     for item in sublist:
        if 'data_' in item:
           index = count % dist_count
           folder_list = folders[index]
           folder_list.append('%s/%s'%(directory,item))
           count += 1

#function to be executed parallely
def execute(directories,pid):
   final_map=OrderedDict(list())
   print(directories)
   for directory in directories:
      samplefile=open(directory+'/sample_data2.csv','w')
      
      for d,s,f in os.walk(directory):
         if d=="*.py":
            pass
                
         else:
            print "Processing [%s] : %s"%(pid,d)
            mydict=OrderedDict(list())
            for f1 in f:
	    #print "Processing [%s] : %s"%(pid,f1)
               if f1=="sample_data2.csv" or f1=="sample_data.csv":
		          pass
               else:
                  mfile = open('%s/%s'%(d,f1))
                  reader = csv.reader(mfile,delimiter=',')
                  t=0
                  headers = None
                  for line in reader:
                     date_value = str(line[6])
                     ch_value   = line[5]
                     date,time = date_value.split(" ")
           
                     if date not in mydict:
                        mydict[date] = []
		   
                     mydict.get(date).append((line[2],ch_value))
      headerlist=['datestamp']

      s = OrderedDict()
      for k,v in mydict.items():
         for item in v:
            t1,t2=item
            if t1 not in s:
               s[t1]=[]

      for k,v in s.items():
         headerlist.append(k)

      samplefile.write('%s\n'%','.join(headerlist))

      for k,v in mydict.items():
         datevalue=k
    
         data_list = [datevalue]
         for item3 in s:
            x=0.0
            for item in v:
               t1,t2=item
               if item3==t1:
                   x+=float(t2)
            
            data_list.append(str(x))
                    
      samplefile.write('%s\n'%','.join(data_list))
      samplefile.close()

      f2 = open(directory+'/sample_data2.csv', 'rU')
      reader = csv.reader(f2,delimiter=',')

      y=OrderedDict()
      i=0
      for line in reader:
        
         if i==0:
            for a in range (len(line)):
               if a==0:
                  pass
               else:
                  if line[a] not in y:
                     y[line[a]]=[]
            i+=1
            pass
         else:
            
            if line[0] not in final_map:
               final_map[line[0]]=[]
            
            
            a=1
            for item in y:
                
               final_map.get(line[0]).append((item,line[a]))
               a+=1
	    break
      os.remove(directory+'/sample_data2.csv')
   return final_map

final_map2 = OrderedDict(list())

def reducer(result):
   for k,v in result:
      if k not in final_map2:
         final_map2[k]=[]
      final_map2.get(k).append(v)

final_map3 = OrderedDict(list())
my_list=sorted(final_map2.keys())
    

for k1 in my_list:
    for k,v in final_map2.items():
        if k1==k:
            final_map3[k]=[]
            for item in v:
                t1,t2=item
                final_map3.get(k).append((t1,t2))

s1= OrderedDict()
for k,v in final_map3.items():
    for item in v:
        t1,t2=item
        if t1 not in s1:
            s1[t1]=[]


for k,v in s1.items():
    print(k,v)

for item2 in s1:
   for k,v in final_map3.items():
      x=0
      for item in v:
         t1,t2=item
	 if t1==item2:
	    x+=1
      if x==0:
         final_map3.get(k).append((item2,0))





f1 = open('/Users/z001lbr/Desktop/sprint2/final_output.csv','w')
headerlist=['datestamp']
    
for k,v in s1.items():
   if k == '':
      pass
   else:
      headerlist.append('store_'+str(k))


f1.write('%s\n'%','.join(headerlist))


for k,v in final_map3.items():
   datevalue=k
   data_list =[datevalue]

   for item in v:
      t1,t2=item
      data_list.append(str(t2))
      print(data_list)
   f1.write('%s\n'%','.join(data_list))

f1.close()



folderDistributor('/Users/z001lbr/Desktop/sprint2')
pp = ParallelRunner(execute,reducer,folders,dist_count)
pp.runThreads()
pp.reducer()


