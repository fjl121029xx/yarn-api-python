#!/usr/bin/env python
# -*- coding:utf-8 -*-
__author__ = 'fjl'
import json
import urllib.request
import datetime
from bs4 import BeautifulSoup
import time
import sys
import threading
import datetime
from apscheduler.schedulers.blocking import BlockingScheduler


def demo():
    response = urllib.request.urlopen("http://hbi-aliyun-02.ali-bj:8088/proxy/application_1577946422212_0007/")
    jsonstring = response.read().decode()
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
                #  job 执行不能超过2.5 min
                if (duration == '2.5 min'):
                    # /proxy/application_1577946422212_0007/jobs/job/kill/?id=
                    response = urllib.request.urlopen(
                        "http://hbi-aliyun-02.ali-bj:8088/proxy/application_1577946422212_0007/jobs/job/kill/?id=" + jodId)
                    jsonstring = response.read().decode()
                    print("job 执行不能超过2.5 min , jodId（" + jodId + "） 被restful 杀死")
                    soup = BeautifulSoup(jsonstring, "html.parser")
                    print(soup.find("span", {"class": "description-input"}))
            break
    return activeJobList


def yarnscheduler():
    alist = demo()
    if (len(alist) < 1):
        print("没有active job %s" % alist)
    else:
        print("active job size %d %s" % (len(alist), str(alist)))


if __name__ == '__main__':
    # timer = threading.Timer(1, scheduler)
    # timer.start()
    scheduler = BlockingScheduler()
    job = scheduler.add_job(yarnscheduler, trigger='interval', seconds=3)  # 每隔5s执行一次func
    scheduler.start()
