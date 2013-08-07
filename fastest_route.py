#### mike mommsen
#### august 2013
#### sp-tag from single source for gtfs data
####
#### question to mikemommsen@gmail.com

import datetime
import time
from collections import defaultdict
import sys
import heapq
import shelve

INSERTSTR = 'insert into raw_graph values ("'

def parseoutput(infilepath, has_headers):
    """parse a | seperated file from sqlite"""
    # open up the file
    f = open(infilepath)
    # this is my way to handle some files having headers and some not
    if has_headers == True:
        # read off the headers 
        x = f.readline()
    # for each row in infile strip off newlines and split
    mylist = [r.strip().split('|') for r in f]
    return mylist

def processtime(intime):
    """processes the input time into a datetime object"""
    # just setting it to a day
    # actually a problem because there are quite a few morning ones
    # so we are not accurate in the morning
    # trains that run overnight are actually a problem for me with how i define time
    # so i am not allowing it making my results invalid in the morning hours
    year, month, day = 2013, 8, 1
    hour, minute, second = map(int, intime.split(':'))
    if hour >= 24:
        # set the hour to the remainder because datetime objects limit to 24  
        day += 1
        hour = hour % 24
    return datetime.datetime(year,month,day,hour,minute,second)

def processtransitlist(inlist):
    """process list of edges with times into dict of dict of dicts"""
    # handy defaultdict takes out a lot of the work
    mydict = defaultdict(dict)
    # loop through each row in inlist
    for start_time, end_time, from_node, to_node in inlist:
        # send the times to the processtime function
        start_time = processtime(start_time)
        end_time = processtime(end_time)
        # if the to_node is not in the dict for from node then create blank list
        if to_node not in mydict[from_node]:
            mydict[from_node][to_node] = []
        # add the times to the list for the from and to node
        mydict[from_node][to_node].append((start_time, end_time))
    # loop through each dict of dict and sort its time list to save time later
    for i in mydict:
        for j in mydict[i]:
            mydict[i][j] = sorted(mydict[i][j], key=lambda x: x[1])
    return dict(mydict)

def looper(graph, indict, stoptime, visited):
    """return the key with the minimum value from the indict that has not
    been visited"""
    # this is the function that i was using before heapq
    # heapq is 20 times faster so this is useless
    for k, v in sorted(indict.iteritems(), key = lambda x: x[1]):
        if k not in visited and k in graph:
            return k
    else:
        return None

def findnext(subgraph,neighbor, startTime):
    """for a specific edge find the minimum time from edge weight dict"""
    # grab the timelist from subgraph going to neighbor
    timelist = subgraph[neighbor]
    # if clause for when its a walking edge
    if timelist == 'walking':
        endtime = startTime + datetime.timedelta(0,300)
    # otherwise it is a subway edge
    else:
        # loop through (list is sorted on endtime) and return first one with
        # with departuretime greater than or equal to startTime
        for s, e in timelist:
            if s >= startTime:
                return e
        # if loop above returns nothing that means there are no valid time
        # so is this needed? or would python take care of it for me
        else:
            endtime = None
    return endtime

#def ride_the_train(graph, timeAllowed):
    

def sp_tag_single_source(graph, startNode, startTime, timeAllowed, dontvisit):
    """find all nodes connected to startNode starting at startTime for time allowed"""
    # create some blank dicts and heap for good fun looping
    mydict = {}  
    pathdict = {}
    mydict[startNode] = startTime
    pathdict[startNode] = []
    stoptime = startTime + timeAllowed
    # have you seen how fast this is?
    heap = []
    # if there is still a node to process
    while startNode:
        # create a subgraph with all connections from that startNode
        subgraph = graph[startNode]
        # loop through each neighboring station (usually only one or two)
        for neighbor in subgraph.iterkeys():
            # make sure neighbor is not in same station complex and has trains that leave it
            if neighbor not in dontvisit and neighbor in graph:
                # send the subgraph, node, time to findnext to get the next train connection
                endtime = findnext(subgraph, neighbor, startTime)
                # if there is a train and the train is before the stoptime and faster than previous times
                if endtime and endtime < mydict.get(neighbor, stoptime): 
                    # add it to the dictionary of fastest time to each neighbor
                    mydict[neighbor] = endtime 
                    # throw it at the heap which is our queue for the next startNode
                    heapq.heappush(heap, (endtime, neighbor))
                    # this saves the path to each node which is nice for maps etc
                    # pathdict[neighbor] = pathdict[startNode] + [(neighbor, endtime)]
        # if there are more nodes to go to
        if heap:
            # grab the first value from heap, which is the one with smallest time value
            startTime, startNode = heapq.heappop(heap)
        else:
            break
    return mydict

def findpossibletimes(graph, node):
    """flatten the dict to find all possible times for departure"""
    # this function is called for each node to find possible departure times
    # then each time will be fed to the sp_tag function with the node
    mylist = []
    # loop through the keys
    for edge in graph[node].iterkeys():
        # all these walking if clauses make me think it should be an object
        if graph[node][edge] != 'walking':
            # loop through every start and endtime
            for startTime, endTime in graph[node][edge]:
                # add each start time to mylist
                mylist += [startTime]
    return mylist

def run(inputnodes, transfers, timeAllowed):
    """garbage function to feed shit"""
    # throw the input text file to parseoutput to create a basic list
    graph = parseoutput(inputnodes, False)
    # then send it to processtransitlist to turn it into a dictionary
    graph = processtransitlist(graph)
    # process the transfers, which have headers in this case (stupid me)
    transfers = parseoutput(transfers, True)
    # add the walking transfers to the graph maybe should be different function
    for fromnode,tonode in transfers:
        if fromnode in graph:
            graph[fromnode][tonode] = 'walking'
    # now we are looping to start sending things to sp_tag
    for node in [x for x in sorted(graph) if x[0] in ['R','S']]:
        # start a timer so we can check out speeds
        t1 = time.time()
        # make it so it can not visit other nodes that it is connected to
        dontvisit = [y for x, y in transfers if x == node]
        # loop through each possible transfer time
        for t in findpossibletimes(graph, node):
            #tempgraph = prunegraph(graph, t, timeAllowed)
            outdict = {}
            # send to the sp_tag function
            outdict[t] = sp_tag_single_source(graph, node, t, timeAllowed, dontvisit)
            for i,j in outdict[t].items():
                print INSERTSTR + '","'.join(map(str, [node, t, i, j, j - t])) + '");'
    return True

def main():
    # take the args
    inputnodes = sys.argv[1]
    transfers = sys.argv[2]
    timeAllowed = sys.argv[3]
    # timeallowed needs to be a timedelta object
    timeAllowed = datetime.timedelta(0,int(timeAllowed))
    # i am having this print a sql file that can by piped
    print 'begin;'
    # throw everything at run
    run(inputnodes, transfers, timeAllowed)
    print 'commit;'
    print True

if __name__ == '__main__':
    main()
