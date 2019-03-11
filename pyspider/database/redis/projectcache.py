#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Binux<roy@binux.me>
#         http://binux.me
# Created on 2015-05-16 21:01:52

import six
import time
import json
import redis
import logging
import itertools

from pyspider.libs import utils
from pyspider.database.base.projectdb import ProjectDB as BaseProjectDB


class ProjectCache(BaseProjectDB):
    UPDATE_PROJECTS_TIME = 10 * 60
    __prefix__ = 'projectcache_'

    def __init__(self, host='localhost', port=6379, password=None, db=0):
        self.redis = redis.StrictRedis(host=host, port=port, password=password, db=db)
        try:
            self.redis.scan(count=1)
            self.scan_available = True
        except Exception as e:
            logging.debug("redis_scan disabled: %r", e)
            self.scan_available = False

    def _gen_key(self, project, taskid):
        return "%s%s_%s" % (self.__prefix__, project, taskid)

    def _gen_status_key(self, project, status):
        return '%s%s_status_%d' % (self.__prefix__, project, status)

    def _parse(self, data):
        if six.PY3:
            result = {}
            for key, value in data.items():
                if isinstance(value, bytes):
                    value = utils.text(value)
                result[utils.text(key)] = value
            data = result

        for each in ('schedule', 'fetch', 'process', 'track'):
            if each in data:
                if data[each]:
                    data[each] = json.loads(data[each])
                else:
                    data[each] = {}
        if 'status' in data:
            data['status'] = int(data['status'])
        if 'lastcrawltime' in data:
            data['lastcrawltime'] = float(data['lastcrawltime'] or 0)
        if 'updatetime' in data:
            data['updatetime'] = float(data['updatetime'] or 0)
        return data

    def _stringify(self, data):
        for each in ('schedule', 'fetch', 'process', 'track'):
            if each in data:
                data[each] = json.dumps(data[each])
        return data

    def get_project_delay_level(self, project=None):
        return int(self.redis.get(project)) if self.redis.get(project) is not None else 0

    def set_project_delay_level(self, project=None, value=1):
        self.redis.set(project, value)

