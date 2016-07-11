#!usr/bin/env python
# -*-coding:utf-8-*-
"""
author:wubaichuan

"""
import logging
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

log = logging.getLogger('upload')
log.setLevel(logging.DEBUG)

fh = logging.FileHandler('upload.log')
fh.setLevel(logging.INFO)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

formatter = logging.Formatter('%(threadName)s - %(asctime)s - %(name)s - %(levelname)s  %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)

log.addHandler(ch)
log.addHandler(fh)
