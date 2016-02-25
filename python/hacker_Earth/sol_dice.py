
def partition(list, start, end):
    pivot = list[end]                          # Partition around the last value
    bottom = start-1                           # Start outside the area to be partitioned
    top = end                                  # Ditto

    done = 0
    while not done:                            # Until all elements are partitioned...

        while not done:                        # Until we find an out of place element...
            bottom = bottom+1                  # ... move the bottom up.

            if bottom == top:                  # If we hit the top...
                done = 1                       # ... we are done.
                break

            if list[bottom] > pivot:           # Is the bottom out of place?
                list[top] = list[bottom]       # Then put it at the top...
                break                          # ... and start searching from the top.

        while not done:                        # Until we find an out of place element...
            top = top-1                        # ... move the top down.
            
            if top == bottom:                  # If we hit the bottom...
                done = 1                       # ... we are done.
                break

            if list[top] < pivot:              # Is the top out of place?
                list[bottom] = list[top]       # Then put it at the bottom...
                break                          # ...and start searching from the bottom.

    list[top] = pivot                          # Put the pivot in its place.
    return top                                 # Return the split point


def quicksort(list, start, end):
    if start < end:                            # If there are two or more elements...
        split = partition(list, start, end)    # ... partition the sublist...
        quicksort(list, start, split-1)        # ... and sort both halves.
        quicksort(list, split+1, end)
    else:
        return





def maxfn(a,b):
	if a>b:
		return a
	else:
		return b

cases=raw_input()
#tech=a.split("")
#print a[1]

row1 = raw_input()
max = [0] * (len(row1))

#print max

for i in xrange(0,len(row1)):
	max[i] = int(row1[i])
 
#print max

quicksort(max,0,len(max)-1)	

#print max

curr = [0] * (len(row1))

for i in xrange(0,int(cases)-1):
	rowi = raw_input()
	for j in xrange(0,len(rowi)):
		curr[j] = int(rowi[j])
	quicksort(curr,0,len(rowi)-1)
	
	for k in xrange(0,len(rowi)):
		max[k] = maxfn(max[k],curr[k])


print max
