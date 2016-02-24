import os
from sets import Set
import csv
from parallel_framework import ParallelRunner
from collections import OrderedDict
#number of parallel threads
dist_count = 16

folders = [[] for i in range(dist_count)]

#distributing folders
def folderDistributor(filepath):
   for directory,sublist,flist in os.walk(filepath):
     count = 0
     for item in sublist:
        if 'data_' in item:
           index = count % dist_count
           folder_list = folders[index]
           folder_list.append('%s/%s'%(directory,item))
           count += 1

#function to be executed parallely
h=['00', '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23']
def execute(directories,pid):
   final_map=OrderedDict(list())
   for directory in directories:
      samplefile=open(directory+'/sample_data2.csv','w')
      for d,s,f in os.walk(directory):

         mydict=OrderedDict(list())
         if d=="*.py" or d=="*.pyc" or d=="*.csv":
            pass
         else:
            print "Processing [%s] : %s"%(pid,d)
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
                     interval=line[3]
                     date_value = str(line[6])
                     ch_value   = line[5]
                     date,time = date_value.split(" ")
		             p,q,r=time.split(":")
       		         hour=p
		             minu=q
                     if date not in mydict:
                        mydict[date] = []

                     mydict.get(date).append((line[2],ch_value,hour,interval,minu))
      headerlist=['datestamp']

      s = OrderedDict()


      for k,v in mydict.items():
         for item in v:
            t1,t2,t3,t4,t5=item
            if t1 not in s:
               s[t1]=[]

      for k,v in s.items():
         headerlist.append(k)

      samplefile.write('%s\n'%','.join(headerlist))
      data_list=[]
      for k,v in mydict.items():
         datevalue=k
         data_list = [datevalue]
         for item3 in s:
            hour_list=[]
            for item2 in h:
               x=0.0
	           u=0
               min_list15=[]
               min_list30=[]
               min_list45=[]
               min_list60=[]
               y=0
               a=0
               b=0
               c=0
               d=0
               for item in v:
                  t1,t2,t3,t4,t5=item
                  if item3==t1 and item2==t3:
                     if t4=='15':     
		                x+=float(t2)
			            u+=1
		
                     if t4=='1':

                        if float(t5)>=0 and float(t5)<15:
                           min_list15.append(float(t2))
                           a=1
                                
                        if float(t5)>=15 and float(t5)<30:
                           min_list30.append(float(t2))
                           b=1
                                        
                        if float(t5)>=30 and float(t5)<45:
                           min_list45.append(float(t2))
                           c=1
                                                
                        if float(t5)>=45 and float(t5)<60:
                           min_list60.append(float(t2))
                           d=1
                
                        u=a+b+c+d
               if a>0:
                  a=float(max(min_list15))
               if b>0:
                  b=float(max(min_list30))
               if c>0:
                  c=float(max(min_list45))
               if d>0:
                  d=float(max(min_list60))
                    
               y=a+b+c+d
               if y>0:
                  x=y
               if u>0:
                  hour_list.append(x/u)
            
            d=0
            for item in hour_list:
               d+=item

            data_list.append(str(d))

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
  #   os.remove(directory+'/sample_data2.csv')
   return final_map

final_map2 = OrderedDict(list())
def reducer(result):
   #final_map2 = OrderedDict(list())
   for k,v in result.items():
      if k not in final_map2:
         final_map2[k]=[]
      for item in v:
         t1,t2=item
         final_map2.get(k).append((t1,t2))
def writeFile():
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

   for item2 in s1:
      for k,v in final_map3.items():
         x=0
         for item in v:
            t1,t2=item
	    if t1==item2:
	       x+=1
         if x==0:
            final_map3.get(k).append((item2,0))

   f1 = open('/home/sbat/external_drive/mainload/final_output.csv','w')
   headerlist=['datestamp']
    
   for k,v in s1.items():
      #print("writing store names")
      if k == '':
         pass
      else:
         headerlist.append('store_'+str(k))
   print("wote store names")

   f1.write('%s\n'%','.join(headerlist))
   print("wrote headers")

   for k,v in final_map3.items():
      datevalue=k
      data_list =[datevalue]
      for item1 in s1:
         for item in v:
            t1,t2=item
	    if item1==t1:
               data_list.append(str(t2))
	       break

      f1.write('%s\n'%','.join(data_list))
   print("done writing")
   f1.close()



folderDistributor('/home/sbat/external_drive/mainload')
pp = ParallelRunner(execute,reducer,folders,dist_count)
pp.runThreads()
pp.reducer()
writeFile()
#print final_set
#print len(final_set)
