#!/usr/bin/env python
# -*- coding:utf-8 -*-
__author__ = 'fjl'

import config
import json
import urllib.request
import datetime
from bs4 import BeautifulSoup
import time
import sys
import threading
import datetime
from apscheduler.schedulers.blocking import BlockingScheduler

serverUri = config.AppConfig['PRODUCT']['yarnServerUri']


def checkSeesionHealthyByYarn(session_id):
    response = urllib.request.urlopen(serverUri + "?state=RUNNING")
    appsDict = json.loads(response.read())
    apps = appsDict['apps']['app']
    # print(apps)
    for i in range(len(appsDict) + 1):
        # print(apps[i]['id'], apps[i]['state'], apps[i]['name'])
        if session_id == apps[i]['id']:
            return 1
        else:
            return 0


print(checkSeesionHealthyByYarn("application_1575531792760_0001"))
# response = urllib.request.urlopen("http://hbi-aliyun-02.ali-bj:8088/cluster")
#
#
# jsonstring = response.read().decode()
# # print(jsonstring)
# soup = BeautifulSoup(jsonstring, "html.parser")
# app_table = soup.find_all('table', {"id": "apps"})
# # print(tables)
# table_soup = BeautifulSoup(str(app_table), "html.parser")
# # print(table_soup)
# print(table_soup.find_all("tr"))
# # tbody = table_soup.find_all("script")
# # appList = tbody[0].get_text().strip().replace("var appsTableData=[", "").replace("[", "").replace("]", "")
# # print(appList)
# # for table in app_table:
# #     table_soup = BeautifulSoup(str(table), "html.parser")
# #     trs = table_soup.find_all('tr', {"id": "apps"})
# #     for tr in trs:
# #         print(tr)
