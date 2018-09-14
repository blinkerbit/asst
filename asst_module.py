from networkx import Graph
from networkx import DiGraph
from networkx import connected_components
from networkx import find_cycle
from networkx import exception
import networkx as nx

import sys
import logging
import logging.handlers


class DataError(Exception):
    pass


class CyclicDependency(Exception):
    pass


# reads data from file
# first line in the file specifies number of job
# second line gives the number of pipelines
# next line has job related data
def get_data(filename) -> "data:dict(dict),time:dict,pipelines:int":
    count = 0
    with open(filename) as f:
        file_data = f.read().split()
    data = {}
    time = {}
    pipelines = int(file_data[0])

    if pipelines == 0:
        raise Exception("no processor found to execute the jobs")

    for i in file_data[1:]:
        count += 1
        temp = i.split(",")
        if temp[1] != '':
            data[temp[0]] = temp[1].split("-")
        else:
            data[temp[0]] = []
        time[temp[0]] = int(temp[2])

#    if len(data) != int(file_data[0]):
#        print(len(data), file_data[0])
#
#        raise DataError("Improper data format in the file:number of jobs referred:" + file_data[0] + "and actual number " +
#                        str(len(data)) + "donot match")
#
#    if count != len(data):
#        raise Warning("Duplicate data identified. Proceeding with the lastest records")

    return data, time, pipelines


# process the data from file
# checks for constraints from constraint check function
# get the list of grouped jobs and their processing time
def get_info(data: "graph data as dict{dict}", time: dict, pipelines) -> "timeinfo:list,pipelines:int":
    graph = DiGraph(data)
    if set(graph.nodes) ^ set( time.keys()) != set():
        raise DataError(" inconsistent with the node " +str(set(graph.nodes) ^ set( time.keys())))
    
    
    
    if constraint_check(graph):
        time_info = []
        for i in list(connected_components(Graph(data))):
            time_info.append(sum([time[x] for x in i]))
        return time_info, pipelines
def getmin(a):
    j=min(a)
    if j==0:
        check = [x for x in a if x != 0]
        if check==[]:
            return 0
        else:
            return min(check)
    else:
        return j
def get_time(data,time,pipelines):
    p=[0]*pipelines
    graph=DiGraph(data)
    constraint_check(graph)
    jobs=[]
    for i in range(pipelines):
        jobs.append(None)
    done=[]
    gtime=0
    while(True):

        
        
        z=dict(graph.in_degree())

        nodes_ = list((k for k,v in z.items() if v == 0))
        for i in jobs:
            if i  in nodes_:
                nodes_.remove(i)


        while (0 in p) and nodes_!=[] :
            
            i=nodes_.pop()

            pointer=p.index(min(p))
            jobs[pointer]=i
            p[pointer]+=time[i]
        val=getmin(p)
        for i in range(pipelines):
            if p[i]==0:
                continue
            p[i]-= val
            if p[i]==0:
                done.append(jobs[i])
                graph.remove_node((jobs[i]))
                jobs[i] = None

        gtime+=val

        if list(graph.nodes)==[]:
            return gtime+max(p)
    
        
            
        
    
    
# get the least time
def get_least_time(times: list, pipelines: "number of parallel machines:int") -> "time:int":
    if len(times) <= pipelines:
        return max(times)
    time_list = times[: pipelines]
    for i in times[pipelines:]:
        time_list[time_list.index(min(time_list))] += i
    return max(time_list)


# checks for cyclic dependencies
def constraint_check(graph: "DiGraph"):
    try:
        err = "cycle found at :" + str(find_cycle(graph))
        logger.error(CyclicDependency(err))
        raise CyclicDependency(err)
    except exception.NetworkXNoCycle as e:
        return True


# function to unify all the above functions
def get_time_from_data(filename):
    logging.info("reading data from file" + filename)
    try:
        result = get_least_time(*get_info(*get_data(sys.argv[1])))
        logging.info("least time calculated" + str(result))
    except DataError as e:
        logging.error(str(e))
        raise e
    except  CyclicDependency as e:
        logging.error(e)
        raise e

    return result


# adding new constraint check function or writing a nested function inside contraint check will easily extend hard or soft
if __name__ == "__main__":
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logfile = logging.handlers.RotatingFileHandler('./asst.log', maxBytes=1024, )
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logfile.setFormatter(formatter)
    logger.addHandler(logfile)
    print(get_time(*get_data(sys.argv[1])))

    #print(get_time_from_data(sys.argv[1]))





