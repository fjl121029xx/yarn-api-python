#!/usr/bin/env python
# -*- coding:utf-8 -*-
__author__ = 'fjl'
import json
import urllib.request
import datetime

content = '【%s】SESSION共计3个【YQS_Read_App、YQS_Read_App1、YQS_Write_App】\n' \
          '其中\n正常%d个【%s，%s】\n' \
          '异常%个【%s，%s】\n' \
          '异常Msg' % ('ok',
              'createMsg(green_dit)', 'createMsg(write_green_dit)', 'createMsg(red_dit)', 'createMsg(write_red_dit)',
              'createMsg(red_dit)', 'createMsg(write_red_dit)')
print(content)
