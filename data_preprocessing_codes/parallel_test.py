import os
from sets import Set
import csv
from multiprocessing import Process,Queue

#number of parallel threads
dist_count = 1

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
def execute(directories,q,pid):
   myset = Set()
   for directory in directories:
      for d,s,f in os.walk(directory):
         print "Processing [%s] : %s"%(pid,d)
         for f1 in f:
            mfile = open('%s/%s'%(d,f1))
            reader = csv.reader(mfile,delimiter=',')
            for row in reader:
               store_id = row[2]
               myset.add(store_id)
            mfile.close()

   q.put(myset)

#running process in multithreaded manner
def runThreads():
   #mapper
   process_list = []
   for i in range(dist_count):
      q = Queue()
      folder = folders[i]
      p = Process(target=execute,args=(folder,q,i))
      p.start()
      process_list.append((p,q))

   final_set = Set()

   #reducer
   for p,q in process_list:
      myset = q.get()
      for item in myset:
         final_set.add(item)

      p.join()

   print final_set
   print len(final_set)

folderDistributor('/home/sbat/external_volume/mainload')
runThreads()
