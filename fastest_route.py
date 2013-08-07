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
    f = open(infilepath)
    # this is my way to handle some files having headers and some not
    if has_headers == True:
        x = f.readline()
    mylist = [r.strip().split('|') for r in f]
    return mylist

def processtime(intime):
    """processes the input time into a datetime object"""
    # just setting it to a day
    # actually a problem because there are quite a few morning ones
    # so we are not accurate in the morning
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
        # set 
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
    mydict = {}  
    pathdict = {}
    mydict[startNode] = startTime
    pathdict[startNode] = []
    stoptime = startTime + timeAllowed
    # have you seen how fast this is?
    heap = []
    # below makes it so you cant walk across the station because that is stupid
    # but maybe not a good idea because you do it anyway
    # it is also slightly slower, and will not matter with post processing
    # graph[startNode] = {k: v for k, v in graph[startNode].items() if v != 'walking'}
    while startNode:
        subgraph = graph[startNode]
        for neighbor in subgraph.iterkeys():
            if neighbor not in dontvisit and neighbor in graph:
                endtime = findnext(subgraph, neighbor, startTime)
                if endtime and endtime < mydict.get(neighbor, stoptime): 
                    mydict[neighbor] = endtime 
                    heapq.heappush(heap, (endtime, neighbor))
                    # pathdict[neighbor] = pathdict[startNode] + [(neighbor, endtime)]
        if heap:
            startTime, startNode = heapq.heappop(heap)
        else:
            break
    return mydict

def findpossibletimes(graph, node):
    """flatten the dict to find all possible times for departure"""
    # this script is called for each node to find possible departure times
    # then each time will be fed to the sp_tag function with the node
    # blank list
    mylist = []
    # loop through the keys
    for edge in graph[node].iterkeys():
        # all these walking if clauses make me think it should be an object
        if graph[node][edge] != 'walking':
            for startTime, endTime in graph[node][edge]:
                # add each start time to mylist
                mylist += [startTime]
    return mylist

# idea to only send the part of the graph within the time contraint
# this does not seem to be working and is not speeding it up at all
def prunegraph(graph, starttime, timeallowed):
    outgraph = dict(graph.items())
    endtime = starttime + timeallowed
    for node in outgraph.iterkeys():
        for edge in outgraph[node]:
            # print outgraph[node][edge]
            if outgraph[node][edge] != 'walking':
                for count,(x, y) in enumerate(outgraph[node][edge]):
                    if x < starttime and y > endtime:
                        del outgraph[node][edge][count]
    return outgraph

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
        # throw the outdict into the shelve
        #db[node] = outdict
        # we could speed this up slightly by syncing less often
        #db.sync()
        # let people know that a node is finished and the time it took
        #print node, time.time() - t1
    return True

def main():
    # take the args
    inputnodes = sys.argv[1]
    transfers = sys.argv[2]
    timeAllowed = sys.argv[3]
    # timeallowed needs to be a timedelta object
    timeAllowed = datetime.timedelta(0,int(timeAllowed))
    # i should actually feed the db in the right way, but no one else likes shelve
    # would be more practical throwing it to sqlite anyway
    global db
    print 'begin;'
    #db = shelve.open('tester2.db')
    # throw everything at run
    run(inputnodes, transfers, timeAllowed)
    print 'commit;'
    #db.close()
    print True

if __name__ == '__main__':
    main()
