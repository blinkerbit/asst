# asst
import networkx as nx
import sys
class DataError(Exception):
    pass
class CyclicDependency(Exception):
    pass


def get_data(filename):
    count=0
    global proc

    with open(filename) as f:
        k=f.read().split()
    data={}
    time={}

    pipelines=int(k[1])

    for i in k[2:]:
        count+=1
        temp=i.split(",")

        if temp[1]!='':
            data[temp[0]]=temp[1].split("-")
        else:
            data[temp[0]]=[]
        time[temp[0]]=int(temp[2])


    if len(data)!=int(k[0]):
        print(len(data),k[0])
        raise DataError("improper data format in the file:number of jobs referred:" +k[0]+"and actual number "+str(len(data))+"donot match")
    if count!=len(data):
        raise Warning("duplicate data identified. considering the lastest records")

    return data,time,pipelines

def get_info(data:"dict{dict}",time:dict,pipelines):
    if contraint_check(data):
        time_info=[]
        for i in list(nx.connected_components(nx.Graph(data))):
            time_info.append(sum([time[x] for x in i]))
        return time_info,pipelines

def get_least_time(times:list,m:"int :number of parallel machines"):
    if len(times )<=m:
        return max(times)
    a=times[:m]
    for i in times[m:]:
        a[a.index(min(a))]+=i
    return max(a)


def contraint_check(data:"dict{dict}"):
    g=nx.Graph(data)
    try:
        print ("cycle found at :",nx.find_cycle(g))

    except nx.exception.NetworkXNoCycle:
        return True

def get_time_from_data(filename):
    data = get_data(sys.argv[1])
    info = get_info(*data)
    return get_least_time(*info)


if __name__ == "__main__":
    print(get_time_from_data(sys.argv[1]))





