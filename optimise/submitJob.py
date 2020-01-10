#!/usr/bin/env python
# -*- coding:utf-8 -*-
__author__ = 'fjl'

import json
import requests

headers = {
    'Content-Type': 'application/json',
    'X-Requested-By': 'admin'
}
data = {
    'code': "select 19",
    'kind': "sql"
}
# 172.20.44.6
# bi-olap1.sm02
response = requests.post("http://bi-olap1.sm02:8999/sessions/" + "8439" + '/statements', data=json.dumps(data),
                         headers=headers)
print(response.text)
