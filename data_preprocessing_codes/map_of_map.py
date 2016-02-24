#Here with the below code we are consolidating energy consumption in KW for each submeter date-wise.
#our data for some dates are at 15 min interval and for some at 1 min interval, our objective is to merge this data date-wise.
#if the data is at 15 min interval consolidation for the day will be made by following process:
#    -For an hour the total KW consumption is computed as the : Average of the of 15 minute intervals.
#    -For an hour the total KW consumption is computed as the : Average of the of 15 minute intervals.
#    -Total for the day : Sum of all the hourly averages.
#if the data is at 1 min interval consolidation for the day will be made by following process:
#    -Compute average value for each 15 minute interval.
#    -For an hour the total KW consumption is computed as the : Average of the of 15 minute intervals.
#    -Total for the day : Sum of all the hourly averages.


import os
from sets import Set
import csv
from parallel_framework import ParallelRunner
from collections import OrderedDict
import sys, traceback
#number of parallel threads
dist_count = 8

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

#making list for each hour
h=['00', '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23']

#function to be executed parallely
#for each date there is a folder containg data for various Submeters and each folder is containing various files

def execute(directories,pid):
    final_map=OrderedDict(list()) # map containg consolidated data for each date
    list_of_sub=OrderedDict()    #contating all unique set Submeters
    for directory in directories:
        try:
            #opening a file to merge data of all files inside a date folder
            samplefile=open(directory+'/sample_data_submeter_map.csv','w')
            
            for d,s,f in os.walk(directory):
                mydict=OrderedDict(list())  # mydict is a map_of_map DS containing stores as key, inside each store key there are keys for each submeter
                no_of_sub=OrderedDict()
                date=0
                if d=="*.py" or d=="*.pyc" or d=="*.csv":
                    pass
                else:
                    print "Processing [%s] : %s"%(pid,d)
                    for f1 in f:
                        # ignoring all the temporary files made by us and considereing only original files in a date folder
                        if f1=="sample_data_submeter.csv" or f1=="sample_data_submeter_map.csv" or f1=="sample_data_submeter_map_sprint6.csv":
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
                                submeter=line[1]
                                date,time = date_value.split(" ")
                                p,q,r=time.split(":")
                                hour=p
                                minu=q
                                
                                if submeter not in list_of_sub:
                                    list_of_sub[submeter]=[]
                                # filling map_of_map keys
                                if store_name in mydict:
                                    no_of_sub = mydict.get(store_name)
                                else:
                                    mydict[store_name]={}
                                    no_of_sub = mydict.get(store_name)
                                #filling internal map's keys
                                if submeter in no_of_sub:
                                    outlist = no_of_sub.get(submeter)
                                else:
                                    no_of_sub[submeter]=[]
                                    outlist = no_of_sub.get(submeter)
                
                                outlist.append((ch_value,hour,interval,minu))

            # after making map of a particular date folder write this  into a temporary file "sample_data_submeter_map.csv", such that values of each submeter is consolidated date-wise
            headerlist=['store']
            headerlist.append('date')
            for item in list_of_sub:
                headerlist.append(str(item))
            samplefile.write('%s\n'%','.join(headerlist))
       
       	    for k1,v1 in mydict.items():
                store=str(k1)
                data_list = [store]
                data_list.append(str(date))
                for item3 in list_of_sub:
                    data=[list()]
                    data=v1.get(item3)
                    if data is not None:
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
                            a1=0
                            b1=0
                            c1=0
                            d1=0
                            for item in data:
                      	        t2=item[0]
                                t3=item[1]
                                t4=item[2]
                                t5=item[3]
                                if item2==t3:
                                    if t4=='15':
                                        x+=float(t2)
                                        u+=1
                                    if t4=='1':
                                        if float(t5)>=0 and float(t5)<15:
                                            min_list15.append(float(t2))
                                            a+=1
                                            a1=1
                                                           
                                        if float(t5)>=15 and float(t5)<30:
                                            min_list30.append(float(t2))
                                            b+=1
                                            b1=1
                                        if float(t5)>=30 and float(t5)<45:
                                            min_list45.append(float(t2))
                                            c+=1
                                            c1=1
                                        if float(t5)>=45 and float(t5)<60:
                                            min_list60.append(float(t2))
                                            d+=1
                                            d1=1
                                        u=a1+b1+c1+d1
                    # for 1 min interval data find average at each 15 min interval
                            if a>0:
                                a=float(sum(min_list15)/a)
                            if b>0:
                                b=float(sum(min_list30)/b)
                            if c>0:
                                c=float(sum(min_list45)/c)
                            if d>0:
                                d=float(sum(min_list60)/d)
                                                                                                                                                
                    # then find average of all 15 min interval enteries for an hour
                            y=a+b+c+d
                            if y>0:
                                x=y
                            if u>0:
                                hour_list.append(x/u)
                                                                                                                                                                    
                    # adding all 24 averaged values 1 for each hour of the day
                        d=0
                        for item in hour_list:
                            d+=item
                        data_list.append(str(d))
                    else:
                        data_list.append(str(0))                                                                                                                                                                  
                samplefile.write('%s\n'%','.join(data_list))
            samplefile.close()
            # reading the temporary file made and merging data date-wise for all dates in the final-map
            for d,s,f in os.walk(directory):
                for f1 in f:
                    if f1=="sample_data_submeter_map.csv":
                        mfile = open('%s/%s'%(d,f1),'rU')
                        reader = csv.reader(mfile,delimiter=',')
                        y=OrderedDict()
                        i=0
                        for line in reader:
                            if i==0:
                                for a in range (len(line)):
                                    if a==0:
                                        pass
                                    if a==1:
                                        pass
                                    if a>1:
                                        if line[a] not in y:
                                            y[line[a]]=[]
                                i+=1
                                pass
                            else:
                                if line[1] not in final_map:
                                    final_map[line[1]]=[]
                                a=2
                                for item in y:
                                    final_map.get(line[1]).append((line[0],item,line[a]))
                                    a+=1
        except Exception as ex:
            print(ex)
            print("............."+directory+"..................")
	    traceback.print_exc(file=sys.stdout)
	    pass
    return final_map
final_map3 = OrderedDict(list())


def reducer(result):
    for k,v in result.items():
        if k not in final_map3:
            final_map3[k]=[]
            for item in v:
               t1,t2,t3=item
               final_map3.get(k).append((t1,t2,t3))

# writing the output of reducer function into final_output file
def writeFile():
    final_map5 = OrderedDict(list())
    # sorting the map on the basis of date
    my_list=sorted(final_map3.keys())
    for k1 in my_list:
        for k,v in final_map3.items():
            if k1==k:
                for item in v:
                    t1,t2,t3=item
                    f=(k,t1)
                    if f not in final_map5:
                        final_map5[f]=[]
                    final_map5.get(f).append((t2,t3))
                        
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
                                                                                    
    f1 = open('/home/sbat/external_drive/submeter_short/final_output_maoOfmap.csv','w')
    headerlist=['store']
    headerlist.append("date")
    for k,v in s1.items():
        if k == '':
            pass
        else:
            headerlist.append(str(k))
    f1.write('%s\n'%','.join(headerlist))
    print("wrote headers")
                                                                                                                            
    for k,v in final_map5.items():
        store=k[1]
        date=k[0]
        data_list =[store]
        data_list.append(str(date))
        for item1 in s1:
            for item in v:
                t1,t2=item
                if item1==t1:
                    data_list.append(str(t2))
                                                                                                                                                                    
        f1.write('%s\n'%','.join(data_list))
    print("done writing............................................")
    f1.close()

        
folderDistributor('/home/sbat/external_drive/submeter_short')
pp = ParallelRunner(execute,reducer,folders,dist_count)
pp.runThreads()
pp.reducer()
writeFile()




