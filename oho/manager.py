#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

from bs4 import BeautifulSoup
from urllib import request
import json
import requests
import logging
import config
import time
import os

moduleLogger = logging.getLogger('manager')
sessionStartStateList = ['not_started', 'starting']
sessionAvailableStateList = ['idle', 'busy']
sessionUnavailableStateList = ['shutting_down', 'error', 'dead', 'killed', 'success']

READ_APP_NAME = 'YQS_Read_App'
WRITE_APP_NAME = 'YQS_Write_App'


class LivyServerManager:
    def __init__(self, env, yarnServerManager):
        self.env = env
        self.logger = logging.getLogger('manager.LivYserverManager')
        self.serverUri = config.AppConfig[env]['livyServerUri']
        self.livyServerPath = config.AppConfig[env]['livyServerPath']
        self.sessions = []
        self.startingSessions = []
        self.availableSessions = []
        self.uselessSessions = []
        self.yarnServerManager = yarnServerManager

    '''
        重启 livy server
    '''

    def restartLivyServer(self, waitTimeForCloseAllLivyProcessInMinute):
        self.getAllSessions()
        self.clearAllSessions()
        time.sleep(60)
        os.system(self.livyServerPath + ' stop')
        self.logger.info('stop livy server')
        time.sleep(60 * 2)
        # 获取 livy server 相关的进程列表
        cmd = "ps -ef | grep livy | awk {'print $2'}"
        output = os.popen(cmd)
        pids = output.readlines()
        self.logger.info('kill livy pids: %s' % (pids))
        for pid in pids:
            os.system('kill -9 ' + pid)

        os.system(self.livyServerPath + ' start')
        self.logger.info('start livy server')

        self.yarnServerManager.killAllRunningApps()
        time.sleep(60 * 2)

    '''
        获取 livyserver 所有 sessions
    '''

    def getAllSessions(self):
        self.sessions = []
        try:
            ticks = time.time()
            print(self.serverUri)
            response = request.urlopen(self.serverUri)
            print(
                "↓↓↓↓↓↓ getAllSessions ↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓↓")
            sessionDict = json.loads(response.read())
            self.sessions = sessionDict['sessions']
            for i in range(len(self.sessions)):
                cursession = self.sessions[i]
                print(cursession['appId'])
                print("当前时间戳为:", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()), 'SESSION_ID:', cursession['id'],
                      ', SESSION_NAME:', cursession['name'],
                      ', APPLICATIONID:',
                      cursession['appId']
                      , ', SESSION_STATE:', cursession['state'], ', APPLICATION_STATE:',
                      self.checkSeesionHealthyByYarn(cursession['appId']),
                      ', ActiveJobs: ', self.yarnServerManager.getActiveJobs(cursession['appId'])
                      )
            print(
                "↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑↑")
            # self.logger.info('getAllSessions sessions size: %s' %(self.sessions))
            self.logger.info('getAllSessions sessions size: %s' % (len(self.sessions)))
        except Exception as e:
            self.logger.error('getAllSessions error:', repr(e))

    '''
        对 sessions 进行分类
        20200106 分类加yarn判断，session和yarn对应
    '''

    def classifySessions(self):
        self.startingSessions = []
        self.availableSessions = []
        self.uselessSessions = []

        for i in range(len(self.sessions)):
            if self.sessions[i]['state'] in sessionStartStateList:
                self.startingSessions.append(self.sessions[i])
                continue
            if self.sessions[i]['state'] in sessionAvailableStateList:
                # self.availableSessions.append(self.sessions[i])
                # continue
                if self.checkSeesionHealthyByYarn(self.sessions[i]['appId']) == 'RUNNING':
                    self.availableSessions.append(self.sessions[i])
                else:
                    print("发现一个无用session：无application")
                    self.uselessSessions.append(self.sessions[i])
                continue

            self.uselessSessions.append(self.sessions[i])
        # self.logger.debug('classifySessions startingSessions: %s' %(self.startingSessions))
        # self.logger.debug('classifySessions availableSessions: %s' %(self.availableSessions))
        self.logger.debug('classifySessions uselessSessions: %s' % (self.uselessSessions))

    def checkSeesionHealthyByYarn(self, session_id):
        response = request.urlopen(
            self.yarnServerManager.serverUri + "" + str(session_id) + "/state")
        appsDict = json.loads(response.read())

        # print("session_id", session_id, appsDict['state'])
        if 'RUNNING' == appsDict['state']:
            return 'RUNNING'
        else:
            return 'OTHER'

    def getYQSSessoins(self):
        readingSessionList = []
        writingSessionList = []

        usefulSessions = self.startingSessions + self.availableSessions

        (readingAppList, writingAppList) = self.yarnServerManager.getRunningYQSApps()

        for i in range(len(readingAppList)):
            for i2 in range(len(usefulSessions)):
                readingApp = readingAppList[i]
                session = usefulSessions[i2]
                if readingApp['id'] == session['appId']:
                    readingSessionList.append(session)
        for i in range(len(writingAppList)):
            for i2 in range(len(usefulSessions)):
                writingApp = writingAppList[i]
                session = usefulSessions[i2]
                if writingApp['id'] == session['appId']:
                    writingSessionList.append(session)
            # if usefulSessions[i]['name'] == WRITE_APP_NAME:
            # writingSessionList.append(usefulSessions[i])
        self.logger.debug('YQSSessions read sessions: %s' % (readingSessionList))
        self.logger.debug('YQSSessions write sessions: %s' % (writingSessionList))
        return (readingSessionList, writingSessionList)

    def clearUselessSessions(self):
        headers = {'X-Requested-By': 'admin'}
        for i in range(len(self.uselessSessions)):
            self.logger.warning('clear session: %s' % (self.uselessSessions[i]['id']))
            requests.delete(self.serverUri + str(self.uselessSessions[i]['id']), headers=headers)

    '''
        关闭 livyServer 所有的 sessions
    '''

    def clearAllSessions(self):
        headers = {'X-Requested-By': 'admin'}
        for i in range(len(self.sessions)):
            self.logger.warning('clear session: %s' % (self.sessions[i]['id']))
            requests.delete(self.serverUri + str(self.sessions[i]['id']), headers=headers)

    def createSession(self, type):
        print("==============================createSession==============================")
        '''
        {"archives": [], "driverMemory": "512m", "executorMemory": "2g", "driverCores": 1, "kind": "spark",
         "executorCores": 2, "jars": ["hdfs://cluster/yqs/tools/engine-0.0.1-SNAPSHOT.jar"],
         "conf": {"spark.io.compression.codec": "snappy", "spark.default.parallelism": 12, "spark.rdd.compress": true},
         "pyFiles": [], "numExecutors": 2, "name": "YQS_Write_App", "proxyUser": "yqs", "files": [],
         "heartbeatTimeoutInSecond": 86400, "queue": "default"}
        '''
        sessionConf = {}

        if type == 0:
            sessionConf = config.AppConfig[self.env]['readApp']
            sessionConf["name"] = READ_APP_NAME
        else:
            sessionConf = config.AppConfig[self.env]['writeApp']
            sessionConf["name"] = WRITE_APP_NAME

        headers = {'X-Requested-By': 'admin'}
        try:
            # print(sessionConf)
            response = requests.post(self.serverUri, data=json.dumps(sessionConf), headers=headers)
            print('createSession %s' % (response.text))
            self.logger.warning('createSession %s' % (response.text))
        except Exception as e:
            print('createSession error: %s' % (repr(e)))
            self.logger.error('createSession error: %s' % (repr(e)))

    def removeSessoin(self, sessionId):
        pass

    def isSessionAvailable(self, state):
        pass

    def isSessionUnavailable(self, state):
        pass;

    def createStatements(self, sessionId, code):
        headers = {
            'Content-Type': 'application/json',
            'X-Requested-By': 'admin'
        }
        data = {
            'code': "SELECT  *  FROM   `db_yqs_b_505`.`tbl_pos_bill_pay`  LIMIT  10",
            'kind': "sql"
        }
        try:
            response = requests.post(self.serverUri + sessionId + '/statements', data=json.dumps(data), headers=headers)
            logging.warning(response.text)
        except Exception as e:
            logging.error('error' % (e))

    def getStatements(self, sessionId):
        try:
            response = request.urlopen(self.serverUri + sessionId + '/statements/11')
            statements = json.loads(response.read())
            self.logger.warning('getStatements %s' % (statements))
        except Exception as e:
            self.logger.error('getStatements error:', repr(e))

    def getSessionCompleted(self, sessionId):
        headers = {'X-Requested-By': 'admin'}
        data = {
            'code': 'spark.sql("select 1")',
            'kind': 'spark',
            'cursor': ''
        }
        try:
            response = requests.post(self.serverUri + sessionId + '/completion', data=json.dumps(data), headers=headers)
            logging.warning(response.text)
        except Exception as e:
            logging.error('error' % (e))


class YarnServerManager:
    def __init__(self, env):
        self.env = env
        self.logger = logging.getLogger('manager.YarnServerManager')
        self.serverUri = config.AppConfig[env]['yarnServerUri']
        self.runningAppList = []

    def getRunningApps(self):
        try:
            response = request.urlopen(self.serverUri + '?state=RUNNING')
            appsDict = json.loads(response.read())
            self.runningAppList = appsDict["apps"]["app"]
        except Exception as e:
            self.logger.error('getRunningApps error: %s' % (repr(e)))

    '''
        Get Yaoqianshu related apps in yarn service
    '''

    def getRunningYQSApps(self):
        readingAppList = []
        writingAppList = []

        self.getRunningApps()
        for i in range(len(self.runningAppList)):
            if self.runningAppList[i]['name'] == READ_APP_NAME:
                readingAppList.append(self.runningAppList[i])
            if self.runningAppList[i]['name'] == WRITE_APP_NAME:
                writingAppList.append(self.runningAppList[i])
        self.logger.debug('Running readingApps: %s' % (readingAppList))
        self.logger.debug('Running writingApps: %s' % (writingAppList))
        return (readingAppList, writingAppList)

    def getAppsByName(self, appName):
        pass

    def killAllRunningApps(self):
        self.getRunningYQSApps()
        for i in range(len(self.runningAppList)):
            runningApp = self.runningAppList[i]
            self.logger.warning('kill runninging app: yarn app -kill %s' % (runningApp['id']))
            cmd = 'yarn app -kill ' + runningApp['id']
            os.system(cmd)

    def getActiveJobs(self, session_id):
        response = request.urlopen("http://bi-olap1.sm02:8088/proxy/%s/" % session_id)
        jsonstring = response.read().decode()
        # print(jsonstring)
        soup = BeautifulSoup(jsonstring, "html.parser")
        activeJobList = []
        tables = soup.find_all('table')
        for table in tables:
            if table.get("id") == "activeJob-table":
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
        return len(activeJobList)
