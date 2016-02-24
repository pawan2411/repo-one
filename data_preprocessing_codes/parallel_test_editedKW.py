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
      try:
         samplefile=open(directory+'/sample_dataKW.csv','w')

         for d,s,f in os.walk(directory):
            mydict=OrderedDict(list())
            date=0
            if d=="*.py" or d=="*.pyc" or d=="*.csv":
               pass
            else:
            #print "Processing [%s] : %s"%(pid,d)
               for f1 in f:
    #print "Processing [%s] : %s"%(pid,f1)
                  if f1=="sample_dataKW.csv" or f1=="sample_dataKWH.csv" or f1=="sample_data3.csv":
                     pass
                  else:
                     mfile = open('%s/%s'%(d,f1),'rU')
                     reader = csv.reader(mfile,delimiter=',')
                 
                     headers = None
                     for line in reader:
                        interval=line[3]
                        date_value = str(line[6])
                        ch_value   = line[5]
                        store_name=line[2]
                        date,time = date_value.split(" ")
                        p,q,r=time.split(":")
                        hour=p
                        minu=q
                        if store_name not in mydict:
                           mydict[store_name] = []

                        mydict.get(store_name).append((ch_value,hour,interval,minu))

         headerlist=['datestamp']
         datevalue=str(date)
         data_list = [datevalue]


         for k,v in mydict.items():
            headerlist.append(str(k))
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
               a=0
               b=0
               c=0
               d=0
               a1=0
               b1=0
               c1=0
               d1=0

               for item in v:
                  t1,t2,t3,t4=item
                  if item2==t2:
                     if t3=='15':
                        x+=float(t1)
                        u+=1

                     if t3=='1':
                        if float(t4)>=0 and float(t4)<15:
                           min_list15.append(float(t1))
                           a+=1
                           a1=1

                        if float(t4)>=15 and float(t4)<30:
                           min_list30.append(float(t1))
                           b+=1
                           b1=1
                        if float(t4)>=30 and float(t4)<45:
                           min_list45.append(float(t1))
                           c+=1
                           c1=1
                        if float(t4)>=45 and float(t4)<60:
                           min_list60.append(float(t1))
                           d+=1
                           d1=1
                        u=a1+b1+c1+d1
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

         samplefile.write('%s\n'%','.join(headerlist))
         samplefile.write('%s\n'%','.join(data_list))
         samplefile.close()
      #print(".......sample3 written........."+directory)
         for d,s,f in os.walk(directory):
            for f1 in f:
               if f1=="sample_dataKW.csv":
                  mfile = open('%s/%s'%(d,f1),'rU')
                  reader = csv.reader(mfile,delimiter=',')
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
               else:
	          pass    
 #   os.remove(directory+'/sample_data2.csv')
      except Exception as ex:
         print(ex)
         print("............."+directory+"..................")
	 pass
   return final_map
final_map3 = OrderedDict(list())
def reducer(result):
   for k,v in result.items():
      if k not in final_map3:
         final_map3[k]=[]
         for item in v:
            t1,t2=item
            final_map3.get(k).append((t1,t2))

def writeFile():
   final_map5 = OrderedDict(list())
   my_list=sorted(final_map3.keys())
  
   for k1 in my_list:
      for k,v in final_map3.items():
         if k1==k:
            final_map5[k]=[]
            for item in v:
               t1,t2=item
               final_map5.get(k).append((t1,t2))

   s1= OrderedDict()
   for k,v in final_map5.items():
      for item in v:
         t1,t2=item
         if t1 not in s1:
            s1[t1]=[]
   for item2 in s1:
      for k,v in final_map5.items():
         x=0
         for item in v:
            t1,t2=item
	    if t1==item2:
	       x+=1
         if x==0:
            final_map5.get(k).append((item2,0))

   f1 = open('/home/sbat/external_drive/mainload/final_outputKW.csv','w')
   headerlist=['datestamp']
    
   for k,v in s1.items():
      #print("writing store names")
      if k == '':
         pass
      else:
         headerlist.append('store_'+str(k))

   f1.write('%s\n'%','.join(headerlist))
   print("wrote headers")

   for k,v in final_map5.items():
      datevalue=k
      data_list =[datevalue]
      for item1 in s1:
         for item in v:
            t1,t2=item
	    if item1==t1:
               data_list.append(str(t2))
	       break

      f1.write('%s\n'%','.join(data_list))
   print("done writing............................................")
   f1.close()


folderDistributor('/home/sbat/external_drive/mainload')
pp = ParallelRunner(execute,reducer,folders,dist_count)
pp.runThreads()
pp.reducer()
writeFile()
