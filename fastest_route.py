#### mike mommsen
#### august 2013
#### sp-tag from single source for gtfs data
#### to run do something like : python fastest_route.py gtfs_edges.txt transfers.txt 3600
#### gtfs_edges.txt is just the stop_times.txt file joined on itself on stop_sequence = stop_sequence -1
#### outputs basic sql to input into a database and actually do some work with
#### question to mikemommsen@gmail.com

import datetime
import time
from collections import defaultdict
import sys
import heapq
import shelve
from itertools import groupby

# string that is the start of my insert string
INSERTSTR = 'insert into raw_graph values ("'
GOOD_TIMES = [0,4,8,12,16,20]
GOOD_TIMES = [datetime.timedelta(t) for t in GOOD_TIMES]

# endnodes are needed to know when the station should not be added to the heap and searched. this could be done in a less static way but it was not. gfy.
ENDNODES = ['101','101N','101S','201','201N','201S','247','247N',
	'247S','257','257N','257S','301','301N','301S','401',
	'401N','401S','501','501N','501S','601','601N','601S',
	'701','701N','701S','A02','A02N','A02S','A65','A65N',
	'A65S','D01','D01N','D01S','D43','D43N','D43S','E01',
	'E01N','E01S','F01','F01N','F01S','G05','G05N','G05S',
	'H11','H11N','H11S','H15','H15N','H15S','L29','L29N',
	'L29S','M01','M01N','M01S','M23','M23N','M23S','R01',
	'R01N','R01S','R45','R45N','R45S']

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
    innode = startNode
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
                    if neighbor not in ENDNODES:
                        heapq.heappush(heap, (endtime, neighbor))
                    # this saves the path to each node which is nice for maps etc
                    # pathdict[neighbor] = pathdict[startNode] + [(neighbor, endtime)]
        # if there are more nodes to go to
        if heap:
            # grab the first value from heap, which is the one with smallest time value
            startTime, startNode = heapq.heappop(heap)
        else:
            break
    del mydict[innode]
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
    
def groupStations(transfersList):
    mylist = []
    for key,group in groupby(sorted(transfersList), key=lambda x: x[0]):
        myset = set([x for a in group for x in a])
        if myset not in mylist:
            mylist.append(myset)
    return sorted([sorted(x) for x in mylist])
    
def processoutdict(indict, transferList):
    mydict = defaultdict(list)
    stationGroups = groupStations(transferList)
    for fromnode, starttime in indict:
        values = indict[fromnode, starttime]
        for tonode, arrivaltime in values.items():
            tonode = [x[0] for x in stationGroups if tonode in x][0]
            mydict[tonode] += [[starttime, arrivaltime]]
    for tonode in mydict:
        for count, (starttime, arrivaltime) in enumerate(mydict[tonode]):
            for x,y in mydict[tonode]:
                if x > starttime and y < arrivaltime:
                    del mydict[tonode][count]
                    break
    outdict = {}
    for tonode in mydict:
        outdict[tonode] = []
        loopinglist = [(a.hour, a , b) for a,b in sorted(mydict[tonode]) if a.day == 1 and b.day == 1]
        for h in range(0,24,4):
            timelist = [(y,z) for x,y,z in loopinglist if h <= x < (h + 4)]
            counter, starttime = 0, datetime.datetime(2013,8,1, h)
            if timelist:
                penalty = (starttime + datetime.timedelta(0,14400)) - timelist[-1][0]
                penalty = penalty.total_seconds() / len(timelist)
                lastdeparture = starttime
                for departuretime, arrivaltime in sorted(timelist):
                    wait = (departuretime - lastdeparture).total_seconds() + penalty
                    averagewait = (arrivaltime - departuretime).total_seconds() + (wait / 2)
                    counter += (wait / 14400.) * averagewait
                    lastdeparture = departuretime
            # this is the time after the last subway until the end of the 4 hours
            
                outdict[tonode] += [[starttime,datetime.timedelta(0,int(counter))]]
    return outdict
        
def run(inputnodes, transfers, timeAllowed):
    """garbage function to feed shit"""
    # throw the input text file to parseoutput to create a basic list
    graph = parseoutput(inputnodes, False)
    # then send it to processtransitlist to turn it into a dictionary
    graph = processtransitlist(graph)
    # process the transfers, which have headers in this case (stupid me)
    transfers = parseoutput(transfers, True)
    loopingnodes = groupStations(transfers)
    # add the walking transfers to the graph maybe should be different function
    for fromnode,tonode in transfers:
        if fromnode in graph:
            graph[fromnode][tonode] = 'walking'
    # now we are looping to start sending things to sp_tag
    rowid = 0
    # below is to do just a limited run
    # loopingnodes = [x for x in loopingnodes if '127N' in x]
    for group in loopingnodes:
        outdict = {}
        for node in (x for x in group if x in graph):
            # start a timer so we can check out speeds
            t1 = time.time()
            # loop through each possible transfer time
            for t in findpossibletimes(graph, node):
                # send to the sp_tag function
                outdict[node, t] = sp_tag_single_source(graph, node, t, timeAllowed, list(group))                    
        outdict = processoutdict(outdict, transfers)
        for i in sorted(outdict):
            for starttime, counter in sorted(outdict[i]):
                if counter.total_seconds() < 3601:
                    rowid += 1
                    print INSERTSTR + '","'.join(map(str, [rowid, group[0],i,starttime,counter])) + '");'
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

if __name__ == '__main__':
    main()
