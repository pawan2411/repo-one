import csv
import re
from sets import Set
from collections import OrderedDict
import glob
import os


Folderset = OrderedDict()

Fileset = OrderedDict()

h=['00', '01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23']

mi=['00','15','30','45']

root='/Users/z001lbr/Desktop/sprint4/'
for dirName, subdirList, fileList in os.walk(root):
    if dirName!=root:
        dirName+='/'
        Folderset[dirName]=[]



final_map= OrderedDict(list())

for item5 in Folderset:
    u=OrderedDict()
    n=OrderedDict(list())
    os.chdir(item5)
    print "processing %s"%item5
    t=0
    myfile2=open(item5+'sample_data15.csv','w')
   
    for file in glob.glob("*"):
        if file=="sample_data_submeter.csv" or file=="sample_data15.csv":
            pass
        else:
            print(item5+file)
            f = open(item5+file, 'rU')
            reader = csv.reader(f,delimiter=',')
            headers = None
            for line in reader:
                
                date_value = str(line[6])
                store_name=line[2]
                submeter=line[1]
                interval=line[3]
                ch_value   = line[5]
                t,b=date_value.split(" ")
                p,q,r=b.split(":")
                hour=p
                min=q
                
                if submeter not in u:
                    u[submeter]=[]
                if store_name not in n:
                    n[store_name]=[]
                n.get(store_name).append((submeter,ch_value,hour,interval,min))
    
    headerlist=['store']
    headerlist.append('date')

    for item in u:
        headerlist.append(str(item))
    myfile2.write('%s\n'%','.join(headerlist))
        
    for k1,v1 in n.items():
        store=str(k1)
        
        for item2 in h:
            for item3 in mi:
                data_list = [store]
                f=str(item2+":"+item3)
                data_list.append(str(t)+" "+f)
                for item in u:
                    min_list00=[]
                    min_list15=[]
                    min_list30=[]
                    min_list45=[]
                    a0=0
                    a15=0
                    a30=0
                    a45=0
                    x=0.0
                
                    for item4 in v1:
                        t1,t2,t3,t4,t5=item4
                        if item==t1:
                            if item2==t3:
                    
                                if t4=='15':
                                    if t5==item3:
                                        x=float(t2)

                                if t4=='1':
                                    if float(t5)>=0 and float(t5)<15:
                                        min_list00.append(float(t2))
                                        a0+=1
                
                                    if float(t5)>=15 and float(5)< 30:
                                        min_list15.append(float(t2))
                                        a15+=1
                            
                                    if float(t5)>=30 and float(t5)< 45:
                                        min_list30.append(float(t2))
                                        a30+=1
                                    
                                    if float(t5)>=45 and float(t5)< 60:
                                        min_list45.append(float(t2))
                                        a45+=1
                    
                    if item3=='00' and a0>0:
                        a0=float(sum(min_list00)/a0)
                        x=a0
                    if item3=='15' and a15>0:
                        a15=float(sum(min_list15)/a15)
                        x=a15
                    if item3=='30' and a30>0:
                        a30=float(sum(min_list30)/a30)
                        x=a30
                    if item3=='45' and a45>0:
                        a45=float(sum(min_list45)/a45)
                        x=a45
                        
                    data_list.append(str(x))
                        
                myfile2.write('%s\n'%','.join(data_list))
    myfile2.close()

    f2 = open(item5+'sample_data15.csv', 'rU')

    reader = csv.reader(f2,delimiter=',')

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

final_map3 = OrderedDict(list())
my_list=sorted(final_map.keys())
    
    
for k1 in my_list:
    for k,v in final_map.items():
        if k1==k:
            for item in v:
                t1,t2,t3=item
                f=(k,t1)
                if f not in final_map3:
                    final_map3[f]=[]
                final_map3.get(f).append((t2,t3))

s1=OrderedDict()
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




f1 = open(root+"final_output_15.csv",'w')
headerlist=['store']
headerlist.append("date")
    
for k,v in s1.items():
    if k == '':
        pass
    else:
        headerlist.append(str(k))
f1.write('%s\n'%','.join(headerlist))


for k,v in final_map3.items():
    store=k[1]
    date=k[0]
    data_list =[store]
    data_list.append(str(date))

    for item1 in s1:
        for item in v:
            t1,t2=item
            if item1==t1:
                data_list.append(str(t2))

    print(data_list)
    print("\n")

    f1.write('%s\n'%','.join(data_list))
    
f1.close()



