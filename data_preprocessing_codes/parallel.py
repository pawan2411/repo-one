import os
from sets import Set
import csv
from parallel_framework import ParallelRunner

#number of parallel threads
dist_count = 15

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
def execute(directories,pid):
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
                
    return myset

final_set = Set()

def reducer(result):
    for item in result:
        final_set.add(item)

folderDistributor('/Users/z001lbr/Desktop/sprint2')
pp = ParallelRunner(execute,reducer,folders,dist_count)
pp.runThreads()
pp.reducer()

print final_set
print len(final_set)
