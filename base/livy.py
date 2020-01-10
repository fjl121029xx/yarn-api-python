#!/usr/bin/env python
# -*- coding:utf-8 -*-
__author__ = 'fjl'
from urllib import request
import urllib.request
import json
import requests
import logging
import config
import time
import os
import config

# http://rm-http-address:port/ws/v1/cluster/apps/{appid}/state


response = urllib.request.urlopen("http://172.26.25.148:8088/ws/v1/cluster/apps/application_1577946422212_0019/state")
appsDict = json.loads(response.read())
print(appsDict['state'])
#
# def checkSeesionHealthyByYarn(session_id):
#     response = urllib.request.urlopen("http://172.26.25.148:8088/ws/v1/cluster/apps/?state=RUNNING")
#     appsDict = json.loads(response.read())
#     apps = appsDict['apps']['app']
#     # print(apps)
#     for i in range(len(appsDict) + 1):
#         # print(apps[i]['id'], apps[i]['state'], apps[i]['name'])
#         if session_id == apps[i]['id']:
#             return 1
#         else:
#             return 0
#
#
# moduleLogger = logging.getLogger('manager')
# sessionStartStateList = ['not_started', 'starting']
# sessionAvailableStateList = ['idle', 'busy']
# sessionUnavailableStateList = ['shutting_down', 'error', 'dead', 'killed', 'success']
#
# READ_APP_NAME = 'YQS_Read_App'
# WRITE_APP_NAME = 'YQS_Write_App'
#
# startingSessions = []
# availableSessions = []
# uselessSessions = []
#
# serverUri = config.AppConfig['PRODUCT']['livyServerUri']
# response = request.urlopen(serverUri)
# sessionDict = json.loads(response.read())
# sessions = sessionDict['sessions']
# print("sessions", sessions)
#
# for i in range(len(sessions)):
#     # print("sessions[i] ", sessions[i])
#     if sessions[i]['state'] in sessionStartStateList:
#         startingSessions.append(sessions[i])
#         continue
#     if sessions[i]['state'] in sessionAvailableStateList:
#         if checkSeesionHealthyByYarn(sessions[i]['appId']) == 1:
#             availableSessions.append(sessions[i])
#         else:
#             uselessSessions.append(sessions[i])
#         continue
#     uselessSessions.append(sessions[i])
#
# # availableSessions 是否在yarn有对应的application
#
# #
# print("startingSessions", startingSessions)
# print("availableSessions", availableSessions)
# print("uselessSessions", uselessSessions)
