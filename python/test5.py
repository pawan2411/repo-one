import csv
from sets import Set
from collections import OrderedDict

f = open('test.csv', 'rU')
reader = csv.reader(f,delimiter=',')

m = OrderedDict(list())

count = 0
headers = None
for line in reader:
  if count == 0:
     headers = line
  else:
      date_value = line[0]
      store_value = line[3]
      ch_value   = line[2]
      t=date_value,store_value

      for i in range(len(line)):
        header_name = headers[i]
        data_value  = line[i]

        if header_name == 'datetime':
           if t not in m:
              m[t] = []

        if header_name == 'channel_name':
           m.get(t).append((line[i],ch_value))
  count += 1

f.close()

f = open('test1.csv','w')

s = Set()

for k,v in m.items():
    for item in v:
        t1,t2 = item
        s.add(t1)



headerlist=['datetime','store']

count2=0
for item in s:
    if count2==0:
        pass
    else:
        headerlist.append(item)
        count2+=1

print(headerlist)

f.write('%s\n'%','.join(headerlist))

for k,v in m.items():
    datevalue=k[0]
    store=k[1]
    data_list = [datevalue,store]

    for item in v:
        t1,t2 = item
        data_list.append(str(t2))

    f.write('%s\n'%','.join(data_list))

f.close()
