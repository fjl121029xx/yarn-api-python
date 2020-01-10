#!/usr/bin/env python
# -*- coding:utf-8 -*-

# from apscheduler.schedulers.blocking import BlockingScheduler
import config
import json
import urllib.request
import datetime
import os
from bs4 import BeautifulSoup
import time
import sys
import threading
import datetime
import manager
import scheduler
from manager import LivyServerManager, YarnServerManager
import logging.config
import requests


class HbiYQSOptimize:

    def __init__(self, env, yarnServerManager):
        self.env = env
        self.yarnServerManager = yarnServerManager
        self.livyServerManager = LivyServerManager(env, yarnServerManager)
        self.session = self.livyServerManager.sessions

    def restart_all_session(self):
        print()

    def restart_session_by_name(self, session_name):
        print()


def submitJob(livyServerUri, session_id):
    headers = {
        'Content-Type': 'application/json',
        'X-Requested-By': 'admin'
    }
    data = {
        'code': "show databases",
        'kind': "sql"
    }
    response = requests.post(livyServerUri + "" + str(session_id) + '/statements', data=json.dumps(data),
                             headers=headers)
    print(response.text)


def checkLastJobRunTime(appId):
    response = urllib.request.urlopen("http://bi-olap1.sm02:8088/proxy/%s/" % appId)

    jsonstring = response.read().decode()
    soup = BeautifulSoup(jsonstring, "html.parser")
    activeJobList = []
    tables = soup.find_all('table')
    for table in tables:
        if table.get("id") == "completedJob-table":
            table_soup = BeautifulSoup(str(table), "html.parser")
            trs = table_soup.find_all('tr')
            for tr in trs:
                trui = []
                _soup = BeautifulSoup(str(tr), "html.parser")
                tds = _soup.find_all(name='td')
                jodId = tds[0].get_text().strip()
                trui.append(jodId)
                trui.append(tds[1].find('a')['href'])
                trui.append(tds[2].get_text().strip())
                duration = tds[3].get_text().strip()
                trui.append(duration)
                trui.append(tds[4].get_text().strip())
                trui.append(tds[5].get_text().strip())
                activeJobList.append(trui)
            break
    if len(activeJobList) > 0:
        return activeJobList[0]
    else:
        return []


def main():
    print(
        "■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■")
    print("run check main")
    curTime = int(round(time.time() * 1000))
    print("run check main time", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(curTime / 1000)))

    env = scheduler.parseCommand(sys.argv)
    scheduler.setupLogging('config/logconf.json')

    yarnServerManager = YarnServerManager(env)
    livyServerManager = LivyServerManager(env, yarnServerManager)

    livyServerManager.getAllSessions()
    # session 是否都在
    print('isNoSessionNoApplication? ', isNoSessionNoApplication(livyServerManager))

    curSessions = livyServerManager.sessions

    for i in range(len(curSessions)):
        if curSessions[i]['name'] == 'YQS_Read_App':
            print("↓↓↓↓↓↓ session info ↓↓↓↓↓↓")
            sessionid = curSessions[i]['id']
            appid = curSessions[i]['appId']
            sessionname = curSessions[i]['name']
            sessionstate = curSessions[i]['state']
            if sessionstate in ['idle', 'busy']:
                # 提交job
                print("submitJob is run")
                submitJob(livyServerManager.serverUri, sessionid)
                time.sleep(3)
                lastActiveJob = checkLastJobRunTime(appid)
                print(len(lastActiveJob))
                if len(lastActiveJob) == 6:
                    lastCompleteJobTime = int(
                        time.mktime(time.strptime(lastActiveJob[2], "%Y/%m/%d %H:%M:%S"))) * 1000
                else:
                    lastCompleteJobTime = 0
                dif = (curTime - lastCompleteJobTime) / 1000
                print("session_id :::", sessionid,
                      "\r\napplication_name :::", sessionname,
                      "\r\napplication_id :::", appid,
                      "\r\ncurTime :::", time.strftime("%H:%M:%S", time.localtime(curTime / 1000)),
                      "\r\nlastCompleteJobTime :::",
                      time.strftime("%H:%M:%S", time.localtime(lastCompleteJobTime / 1000)),
                      "\r\ndif :::", dif)
                if dif > 60:
                    print("上一次job执行在60s之前")
                    print("〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓")
                    # print("killApplication")
                    # # killSpark()
                    # killApplication(curSessions[i]['appId'])
                    # time.sleep(3)
                    # print("〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓")
                    # print("killSession and restart")
                    # killSession(curSessions[i]['id'], curSessions[i]['name'], yarnServerManager)
                    killAllSessionAndRestart(livyServerManager)
                    print("〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓〓")

    print(
        "■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■■")


def killSpark():
    cmd = 'jps  |grep Spark |awk \'{print $1}\' | xargs kill -9'
    os.system(cmd)


def killAllSessionAndRestart(livyServerManager):
    print('killAllSessionAndRestart')
    sess = livyServerManager.sessions
    headers = {'X-Requested-By': 'admin'}
    # kill session
    print(
        '-------------------------------------------------------------------------------------------------------------')
    print('kill session')
    res = requests.delete("http://bi-olap1.sm02:8999/sessions/" + str(sess[0]['id']), headers=headers)
    print('kill session', str(sess[0]['id']), res.text)
    res = requests.delete("http://bi-olap1.sm02:8999/sessions/" + str(sess[1]['id']), headers=headers)
    print('kill session', str(sess[1]['id']), res.text)
    time.sleep(1)
    print(
        '-------------------------------------------------------------------------------------------------------------')
    print('kill app')
    print(
        '-------------------------------------------------------------------------------------------------------------')
    os.system('yarn app -kill ' + str(sess[0]['appId']))
    print('yarn app -kill ' + str(sess[0]['appId']))
    os.system('yarn app -kill ' + str(sess[1]['appId']))
    print('yarn app -kill ' + str(sess[1]['appId']))
    time.sleep(1)
    print(
        '-------------------------------------------------------------------------------------------------------------')
    print('kill jps')
    killSpark()
    time.sleep(1)
    print(
        '-------------------------------------------------------------------------------------------------------------')
    restartRead()
    restartWrite()


def killSession(session_id, session_name, yarnServerManager):
    print('session_id %s and session_name %s was killed' % (session_id, session_name))
    headers = {'X-Requested-By': 'admin'}
    resp = requests.delete("http://bi-olap1.sm02:8999/sessions/" + str(session_id), headers=headers)
    print(resp.text)
    print("time.sleep(5)")
    time.sleep(5)
    print('about to restart session_name %s' % session_name)
    restartSession(yarnServerManager, session_name)


def killApplication(appId):
    print('kill runninging app: yarn app -kill %s' % (appId))
    cmd = 'yarn app -kill ' + appId
    os.system(cmd)


def restartSession(livyServerManager, type):
    print("restartSession at", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    sessionconf = {}
    if type == 'YQS_Read_App':
        sessionconf = config.AppConfig[livyServerManager.env]['readApp']
        sessionconf["name"] = 'YQS_Read_App'
    else:
        sessionconf = config.AppConfig[livyServerManager.env]['writeApp']
        sessionconf["name"] = 'YQS_Write_App'

    headers = {'X-Requested-By': 'admin'}
    try:
        # print(sessionConf)
        response = requests.post('http://bi-olap1.sm02:8999/sessions/', data=json.dumps(sessionconf), headers=headers)
        print('createSession %s' % (response.text))
    except Exception as e:
        print('createSession error: %s' % (repr(e)))


def isNoSessionNoApplication(livyServerManager):
    sesses = livyServerManager.sessions
    if len(sesses) == 2:
        return 'green'
    elif len(sesses) == 1:
        if sesses[0]['name'] == 'YQS_Write_App':
            restartRead()
            return 'green'
        if sesses[0]['name'] == 'YQS_Read_App':
            restartWrite()
            return 'green'
        return 'yellow'
    elif len(sesses) == 0:
        restartRead()
        restartWrite()
        return 'green'
    else:
        return 'red'


def restartRead():
    print('restartRead')
    headers = {'X-Requested-By': 'admin'}
    try:
        sessionConf = {"jars": ["hdfs://cluster/yqs/tools/engine-0.0.1-SNAPSHOT.jar"], "pyFiles": [], "files": [],
                       "archives": [], "kind": "spark", "driverMemory": "16g", "driverCores": 8,
                       "executorMemory": "10g",
                       "executorCores": 4, "numExecutors": 35, "queue": "default", "heartbeatTimeoutInSecond": 86400,
                       "proxyUser": None, "conf": {"spark.default.parallelism": 200, "spark.scheduler.mode": "FAIR",
                                                   "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                                                   "spark.rdd.compress": True, "spark.io.compression.codec": "snappy",
                                                   "spark.sql.files.maxPartitionBytes": 536870912,
                                                   "spark.sql.broadcastTimeout": 60, "spark.sql.orc.enabled": True,
                                                   "spark.sql.orc.impl": "native"}, "name": "YQS_Read_App"}
        sessionConf["name"] = 'YQS_Read_App'
        response = requests.post('http://bi-olap1.sm02:8999/sessions/', data=json.dumps(sessionConf), headers=headers)
        print('createSession %s' % (response.text))
    except Exception as e:
        print('createSession error: %s' % (repr(e)))


def restartWrite():
    print('restartWrite')
    headers = {'X-Requested-By': 'admin'}
    try:
        sessionConf = {"jars": ["hdfs://cluster/yqs/tools/engine-0.0.1-SNAPSHOT.jar"], "pyFiles": [], "files": [],
                       "archives": [], "kind": 'spark', "driverMemory": '10g',
                       "driverCores": 4, "executorMemory": '10g', "executorCores": 4,
                       "numExecutors": 10, "queue": 'default', "heartbeatTimeoutInSecond": 86400,
                       "proxyUser": None,
                       'conf': {
                           "spark.default.parallelism": 400,
                           "spark.scheduler.mode": "FAIR",
                           "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                           "spark.rdd.compress": True,
                           "spark.io.compression.codec": "snappy",
                           # "spark.sql.inMemoryColumnarStorage.batchSize": 300000,
                           "spark.sql.files.maxPartitionBytes": 536870912,
                           "spark.sql.broadcastTimeout": 60,
                           "spark.sql.orc.enabled": True,
                           "spark.sql.orc.impl": "native",
                           "spark.sql.orc.enableVectorizedReader": True,
                           "spark.sql.hive.convertMetastoreOrc": True,
                           "spark.sql.orc.filterPushdown": True,
                           "spark.sql.orc.char.enabled": True,
                           "spark.driver.extraJavaOptions": "-Dhdp.version=3.0.1.0-187",
                           "spark.executor.extraJavaOptions": "-Dhdp.version=3.0.1.0-187"}}
        sessionConf["name"] = 'YQS_Write_App'
        response = requests.post('http://bi-olap1.sm02:8999/sessions/', data=json.dumps(sessionConf), headers=headers)
        print('createSession %s' % (response.text))
    except Exception as e:
        print('createSession error: %s' % (repr(e)))


if __name__ == '__main__':
    # timer = threading.Timer(1, scheduler)
    # timer.start()
    # print(time.time())
    # timeScheduler = BlockingScheduler()
    # job = timeScheduler.add_job(main, trigger='interval', seconds=60)  # 每隔5s执行一次func
    # timeScheduler.start()
    # /usr/local/python3/bin/python3 /home/hadoop/livyScheduler/checkLivySession.py -e product
    # python checkLivySession.py -e product
    main()
