#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

from urllib import request
import json
import time
import subprocess
import os
import sys
import getopt
import logging.config
import datetime
from manager import LivyServerManager, YarnServerManager


def parseCommand(argv):
    if len(argv) == 1:
        print('命令行参数错误，请输入 -h 获取更多信息')
        sys.exit()
    try:
        options, args = getopt.getopt(argv[1:], "he:", ["help", "env="])
    except getopt.GetoptError:
        print('命令行参数错误，请输入 -h 获取更多信息')
        sys.exit()
    ret = ''

    for option, value in options:
        if option in ("-h", "--help"):
            print("-e 指定服务环境")
            print("-e dohko 测试线")
            print("-e product 生产线")
            sys.exit()
        if option in ("-e", "--env"):
            ret = value.upper()

    if ret == 'DOHKO' or ret == 'PRODUCT':
        return ret
    else:
        print('命令行参数错误，请输入 -h 获取更多信息')
        sys.exit()


def setupLogging(default_path="logging.json", default_level=logging.INFO, env_key="LOG_CFG"):
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, "r") as f:
            config = json.load(f)
            logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)


def main():
    print("run main")
    env = parseCommand(sys.argv)

    setupLogging('config/logconf.json')
    logger = logging.getLogger('scheduler')

    yarnServerManager = YarnServerManager(env)
    livyServerManager = LivyServerManager(env, yarnServerManager)

    waitTimeForCloseAllLivyProcessInMinute = 5

    # while True:
    try:
        dt = datetime.datetime.now()
        hour = dt.__getattribute__("hour")
        minute = dt.__getattribute__("minute")
        if hour == 0 and minute < waitTimeForCloseAllLivyProcessInMinute:
            livyServerManager.restartLivyServer(waitTimeForCloseAllLivyProcessInMinute)

        # 获取livy所有session
        livyServerManager.getAllSessions()
        livyServerManager.classifySessions()
        livyServerManager.clearUselessSessions()
        (readingSessionList, writingSessionList) = livyServerManager.getYQSSessoins()
        (readingAppList, writingAppList) = yarnServerManager.getRunningYQSApps()

        # 提取 livy sessions  中的 appId
        readingSessionAppIds = []
        writingSessionAppIds = []
        for i in range(len(readingSessionList)):
            readingSessionAppIds.append(readingSessionList[i]['appId'])
        for i in range(len(writingSessionList)):
            writingSessionAppIds.append(writingSessionList[i]['appId'])

        # yarn 中 appliaction 还在运行， livy server 中 对应的session 已经不存在
        # 需要将这些 appliaction kill 释放资源
        for i in range(len(readingAppList)):
            readingApp = readingAppList[i]
            if readingApp['id'] not in readingSessionAppIds:
                logger.warning('killing reading apps: yarn app -kill %s' % (readingApp['id']))
                cmd = 'yarn app -kill ' + readingApp['id']
                os.system(cmd)

        for i in range(len(writingAppList)):
            writingApp = writingAppList[i]
            if writingApp['id'] not in writingSessionAppIds:
                logger.warning('killing writing apps: yarn app -kill %s' % (writingApp['id']))
                cmd = 'yarn app -kill ' + writingApp['id']
                os.system(cmd)

        YQS_READ_SESSION = 0
        YQS_WRITE_SESSION = 1
        if len(readingSessionList) == 0:
            livyServerManager.createSession(YQS_READ_SESSION)
        if len(writingSessionList) == 0:
            livyServerManager.createSession(YQS_WRITE_SESSION)
        # time.sleep(60)
    except Exception as e:
        logger.error('error occured: %s' % (repr(e)))
        # time.sleep(60)


if __name__ == '__main__':
    main()
    # livyServerManager = LivyServerManager(serverConfig['dohko']['livyServerUri'])
    # livyServerManager.uselessSessions =[{'id': 23593}, {'id': 23594}]
    # livyServerManager.clearUselessSessions()
    # yarnServerManager = YarnServerManager(serverConfig['dohko']['yarnServerUri'])
    # livyServerManager.createSession(0)
