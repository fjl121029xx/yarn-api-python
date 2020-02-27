#!/usr/bin/env python
# -*- coding:utf-8 -*-
__author__ = 'fjl'

import json
import requests
from urllib import request
import time

headers = {
    'Content-Type': 'application/json',
    'X-Requested-By': 'admin'
}
data = {
    'code': "SELECT count(*) FROM `db_yqs_b_777777777`.`livy_watcher`",
    'kind': "sql"
}
# 172.20.44.6
# bi-olap1.sm02

sid = 8894
response = requests.post("http://bi-olap1.sm02:8999/sessions/" + str(sid) + '/statements', data=json.dumps(data),
                         headers=headers)
print(response.text)
id = response.json()['id']
print(id)
time.sleep(10)
response = request.urlopen('http://bi-olap1.sm02:8999/sessions/%d/statements/%d' % (sid, id))
statements = json.loads(response.read())
print(statements)
stmt = statements['state']
print('getStatements %s' % (statements['state']))
if 'available' == stmt:
    print(111)