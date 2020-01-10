#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

import time
import sys
import logging.config
from manager import LivyServerManager, YarnServerManager
import scheduler

sessionList = []

print("run restart")
env = scheduler.parseCommand(sys.argv)

scheduler.setupLogging('config/logconf.json')
logger = logging.getLogger('scheduler')

yarnServerManager = YarnServerManager(env)
livyServerManager = LivyServerManager(env, yarnServerManager)

livyServerManager.getAllSessions()
# print(livyServerManager.sessions)
curSessions = livyServerManager.sessions
# kill application
for i in range(len(curSessions)):
    yarnServerManager.logger.warning('kill runninging app: yarn app -kill %s' % (curSessions[i]['appId']))
    cmd = 'yarn app -kill ' + curSessions[i]['appId']
    # os.system(cmd)
    print(cmd)
time.sleep(60)
# kill session
livyServerManager.clearAllSessions()
time.sleep(60)
# restart session
livyServerManager.createSession(0)
time.sleep(60)
livyServerManager.createSession(1)
