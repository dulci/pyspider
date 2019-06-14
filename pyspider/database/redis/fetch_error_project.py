#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:

import six
import time
import json
import redis
import logging
import itertools

from pyspider.libs import utils
from pyspider.database.base.taskdb import TaskDB as BaseTaskDB

class FetchErrorProject(object):
    __prefix__ = 'fetch_error_project_'
    ERROR_CYCLE = 24 * 60 * 60
    CONTINUOUS_FAILURE_NUM = 400

    def __init__(self, host='localhost', port=6379, password=None, db=0):
        self.redis = redis.StrictRedis(host=host, port=port, password=password, db=db)
        try:
            self.redis.scan(count=1)
            self.scan_available = True
        except Exception as e:
            logging.debug("redis_scan disabled: %r", e)
            self.scan_available = False

    def _gen_key(self, project):
        return "%s%s" % (self.__prefix__, project)

    def _parse(self, data):
        if six.PY3:
            result = {}
            for key, value in data.items():
                if isinstance(value, bytes):
                    value = utils.text(value)
                result[utils.text(key)] = value
            data = result
        return data

    def is_fetch_error(self, project):
        fetch_error_project = self._parse(self.redis.hgetall(self._gen_key(project)))
        if fetch_error_project and int(fetch_error_project['error_num']) >= self.CONTINUOUS_FAILURE_NUM and (time.time() - float(fetch_error_project['last_error_fetch_time'])) < self.ERROR_CYCLE:
            return True
        return False

    def set_error(self, project, taskid):
        fetch_error_project = self._parse(self.redis.hgetall(self._gen_key(project)))
        if not fetch_error_project or (time.time() - float(fetch_error_project['last_error_fetch_time'])) > self.ERROR_CYCLE:
            with self.redis.pipeline(transaction=False) as pipeline:
                pipeline.hmset(self._gen_key(project), {'error_num': 1, 'last_error_fetch_time': time.time(), 'taskids': taskid})
                pipeline.execute()
        else:
            error_dict = dict()
            error_dict['last_error_fetch_time'] = time.time() if int(fetch_error_project['error_num']) <= self.CONTINUOUS_FAILURE_NUM else float(fetch_error_project['last_error_fetch_time'])
            error_dict['error_num'] = int(fetch_error_project['error_num']) + 1
            taskids = fetch_error_project['taskids'].split(',')
            taskids.append(taskid)
            error_dict['taskids'] = ','.join(taskids)
            with self.redis.pipeline(transaction=False) as pipeline:
                pipeline.hmset(self._gen_key(project), error_dict)
                pipeline.execute()

    def drop(self, project):
        with self.redis.pipeline(transaction=False) as pipeline:
            pipeline.delete(self._gen_key(project))
            pipeline.execute()