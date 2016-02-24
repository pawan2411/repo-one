from multiprocessing import Process,Queue

class ParallelRunner(object):
   def __init__(self,target_func,reducer_func,data,count):
       self.target_func = target_func
       self.data = data
       self.thread_count = count
       self.reducer_func = reducer_func
       self.process_list = []

   #mapper
   def mapper(self,atomic_data,q,pid):
      result = self.target_func(atomic_data,pid)
      q.put(result)

   #executor
   def runThreads(self):
      for i in range(self.thread_count):
         q = Queue()
         atomic_data = self.data[i]
         p = Process(target=self.mapper,args=(atomic_data,q,i))
         p.start()
         self.process_list.append((p,q))

   #reducer
   def reducer(self):
      for p,q in self.process_list:
         result = q.get()
         self.reducer_func(result)
         p.join()
