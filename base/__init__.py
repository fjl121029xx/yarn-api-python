#!/usr/bin/env python
# -*- coding:utf-8 -*-
__author__ = 'fjl'
import json
import urllib.request
import datetime


# yarn rest api:
# http://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html#Cluster_Writeable_APIs

def getActiveRN(master1, master2):
    activemaster = ""
    response = urllib.request.urlopen("http://" + master1 + "/ws/v1/cluster/info")
    jsonstring = response.read()
    print(jsonstring)
    j1 = json.loads(jsonstring)
    print(master1 + " resourcemanager state is :" + j1['clusterInfo']['haState'])

    response = urllib.request.urlopen("http://" + master2 + "/ws/v1/cluster/info")
    jsonstring = response.read()
    print(jsonstring)
    j2 = json.loads(jsonstring)
    print(master2 + " resourcemanager state is :" + j2['clusterInfo']['haState'])

    if j1['clusterInfo']['haState'] == 'ACTIVE':
        print("active master is " + master1)
        activemaster = master1
    elif j2['clusterInfo']['haState'] == 'ACTIVE':
        print("active master is " + master2)
        activemaster = master2
    else:
        raise Exception("on active resourcemanger in %s,%s " % (master1, master2))
    return activemaster


def getClusterScheduler(activeResourceManger):
    response = urllib.request.urlopen("http://" + master1 + "/ws/v1/cluster/scheduler")
    jsonstring = response.read()
    print(jsonstring)
    jsonarray = json.loads(jsonstring)
    print(jsonarray)
    return jsonarray


def getQueueInfo(queuename, ClusterScheduler):
    jsonarray = ClusterScheduler['scheduler']['schedulerInfo']['queues']['queue']
    print("**************** %s scheduler info :%s" % (queuename, jsonarray))

    print("*********************************************************")

    print("**************** %s scheduler1 info :%s" % (queuename, jsonarray[1]))
    for i in range(0, len(jsonarray)):
        if (jsonarray[i]['queueName'] == queuename):
            print("find queuename:%s info %s" % (queuename, jsonarray[i]))
            return jsonarray[i]


def findSubQueueInfo(queuename, parenetClusterScheduler):
    print("*********************begin findSubQueueInfo:%s**********" % queuename)
    jsonarray = parenetClusterScheduler['queues']['queue']
    for i in range(0, len(jsonarray)):
        if (jsonarray[i]['queueName'] == queuename):
            print("*********************finish findSubQueueInfo:%s**********" % queuename)
            return jsonarray[i]


def clusteMetrics(activeResourceManger):
    response = urllib.request.urlopen("http://" + activeResourceManger + "/ws/v1/cluster/metrics")
    jsonstring = response.read()
    jsonarray = json.loads(jsonstring)
    return jsonarray


if __name__ == "__main__":
    master1 = "172.26.25.146:8088"
    master2 = "172.26.25.148:8088"
    parenetQueue = ['sto', 'dm', 'bd', 'event']
    bdchildrenQueue = ['vip', 'tenhive', 'AthenaSysService', 'default']
    activemaster = getActiveRN(master1, master2)
    clustemetrics = clusteMetrics(activemaster)
    clustemetrics['clusterMetrics']['totalVirtualCores']

    allvcore = clustemetrics['clusterMetrics']['totalVirtualCores']
    allmemory = clustemetrics['clusterMetrics']['totalMB']
    #
    # clusterScheduler = getClusterScheduler(activemaster)
    # bd = getQueueInfo('bd', clusterScheduler)
    # # defaultQueueInfo=findSubQueueInfo('default', bd)
    # # print(defaultQueueInfo['resourcesUsed']['vCores'])
    #
    # currentAllvcore = 0
    # currentAllmemory = 0
    # currentAllvcorePercentage = 0.0
    # currentAllmemoryPercentage = 0.0
    # fo = open("QueueInfo.txt", "a+")
    # now_time = '\'' + datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S') + '\''
    #
    # for i in parenetQueue:
    #     queueInfo = getQueueInfo(i, clusterScheduler)
    #     currentAllvcore = currentAllvcore + queueInfo['resourcesUsed']['vCores']
    #     currentAllvcorePercentage = currentAllvcorePercentage + (queueInfo['resourcesUsed']['vCores'] * 1.0 / allvcore)
    #     currentAllmemory = currentAllmemory + queueInfo['resourcesUsed']['memory']
    #     currentAllmemoryPercentage = currentAllmemoryPercentage + queueInfo['resourcesUsed']['memory'] * 1.0 / allmemory
    #     queuename = '\'root.' + i + '\''
    #     fo.write("insert into yarn_monitor.yarn_vcore_memory_monitor"
    #              "(curr_time,queuename,currentAllvcore,currentAllmemory,"
    #              "currentAllvcorePercentage,currentAllmemoryPercentage) "
    #              "VALUES (%s,%s,%s,%s,%s,%s);\n" % (
    #                  now_time, queuename, currentAllvcore, currentAllmemory, currentAllvcorePercentage,
    #                  currentAllmemoryPercentage)
    #              )
    #
    # for i in bdchildrenQueue:
    #     queueInfo = findSubQueueInfo(i, bd)
    #     currentAllvcore = currentAllvcore + queueInfo['resourcesUsed']['vCores']
    #     currentAllvcorePercentage = currentAllvcorePercentage + (queueInfo['resourcesUsed']['vCores'] * 1.0 / allvcore)
    #     currentAllmemory = currentAllmemory + queueInfo['resourcesUsed']['memory']
    #     currentAllmemoryPercentage = currentAllmemoryPercentage + queueInfo['resourcesUsed']['memory'] * 1.0 / allmemory
    #     queuename = '\'root.bd.' + i + '\''
    #     fo.write("insert into yarn_monitor.yarn_vcore_memory_monitor"
    #              "(curr_time,queuename,currentAllvcore,currentAllmemory,"
    #              "currentAllvcorePercentage,currentAllmemoryPercentage) "
    #              "VALUES (%s,%s,%s,%s,%s,%s);\n" % (
    #                  now_time, queuename, currentAllvcore, currentAllmemory, currentAllvcorePercentage,
    #                  currentAllmemoryPercentage)
    #              )
    #
    # print("currentAllvcore = %s" % currentAllvcore)
    # print("currentAllallmemory = %s" % currentAllmemory)
    # print("currentAllvcorePercentage = %s" % currentAllvcorePercentage)
    # print("currentAllallmemoryPercentage = %s" % currentAllmemoryPercentage)
    #
    # fo.write("insert into yarn_monitor.yarn_vcore_memory_monitor"
    #          "(curr_time,queuename,currentAllvcore,currentAllmemory,"
    #          "currentAllvcorePercentage,currentAllmemoryPercentage) "
    #          "VALUES (%s,'root',%s,%s,%s,%s);\n" % (
    #              now_time, currentAllvcore, currentAllmemory, currentAllvcorePercentage, currentAllmemoryPercentage)
    #          )
    # fo.close()
