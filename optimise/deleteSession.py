#!/usr/bin/env python
# -*- coding:utf-8 -*-
__author__ = 'fjl'

import requests

headers = {'X-Requested-By': 'admin'}
# 172.20.44.6
# 192.168.101.39
# res = requests.delete("http://172.26.25.148:8999/sessions/2", headers=headers)
res = requests.delete("http://bi-olap1.sm02:8999/sessions/8448", headers=headers)
print(res.text)
