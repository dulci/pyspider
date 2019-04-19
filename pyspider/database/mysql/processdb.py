#!/usr/bin/envutils
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Binux<i@binux.me>
#         http://binux.me
# Created on 2014-07-17 18:53:01


import re
import six
import time
import json
import mysql.connector

from pyspider.libs import utils
from pyspider.database.base.taskdb import TaskDB as BaseTaskDB
from pyspider.database.basedb import BaseDB
from .mysqlbase import MySQLMixin, SplitTableMixin


class ProcessDB(MySQLMixin, SplitTableMixin, BaseTaskDB, BaseDB):
    __tablename__ = ''

    def __init__(self, host='localhost', port=3306, database='processdb',
                 user='root', passwd=None):
        self.database_name = database
        self.conn = mysql.connector.connect(user=user, password=passwd,
                                            host=host, port=port, autocommit=True)
        if database not in [x[0] for x in self._execute('show databases')]:
            self._execute('CREATE DATABASE %s' % self.escape(database))
        self.conn.database = database
        self._create_table()

    def _create_table(self):
        tablename = 'processdb'
        if tablename in [x[0] for x in self._execute('show tables')]:
            return
        self._execute('''CREATE TABLE IF NOT EXISTS %s (
              `taskid` varchar(64) NOT NULL,
              `project` varchar(64) NOT NULL COMMENT '站源标识',
              `group` varchar(64) DEFAULT NULL COMMENT '分组',
              `status` int(1) DEFAULT NULL COMMENT '任务状态 1：任务创建，2：任务已发到scheduler2fetcher队列成功，3：任务已发到scheduler2fetcher队列失败，11：fetcher开始处理，12：fetcher处理成功，任务已发送到fetcher2processor队列，13：fetcher处理失败，21：processor开始处理，22：processor处理成功，任务发送到processor2result队列，23：processor处理失败，31：result_worker开始处理，32：result_worker处理完成，33：result_worker处理失败',
              `process` mediumblob DEFAULT NULL COMMENT 'process描述',
              `fetch` mediumblob DEFAULT NULL COMMENT 'fetch描述',
              `fetcher_response_code` int(11) DEFAULT NULL COMMENT 'fetcher返回值',
              `url` varchar(1024) DEFAULT NULL,
              `scheduler_created_time` datetime DEFAULT NULL COMMENT '任务创建时间',
              `scheduler_to_fetcher_time` datetime DEFAULT NULL COMMENT '任务转发给scheduerl2fetcher队列时间',
              `fetcher_begin_time` datetime DEFAULT NULL COMMENT 'fetcher处理开始时间',
              `fetcher_end_time` datetime DEFAULT NULL COMMENT 'fetcher处理结束时间',
              `processor_begin_time` datetime DEFAULT NULL COMMENT 'processor处理开始时间',
              `processor_end_time` datetime DEFAULT NULL COMMENT 'processor处理结束时间',
              `result_worder_begin_time` datetime DEFAULT NULL COMMENT 'result_worker处理开始时间',
              `result_worder_end_time` datetime DEFAULT NULL COMMENT 'result_worker处理结束时间',
              `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
              `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
              PRIMARY KEY (`taskid`,`project`),
              KEY `IDX_TASKID` (`taskid`) USING BTREE,
              KEY `IDX_PROJECT` (`project`) USING BTREE,
              KEY `IDX_URL` (`url`) USING BTREE
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8''' % self.escape(tablename))

    def _parse(self, data):
        for key, value in list(six.iteritems(data)):
            if isinstance(value, (bytearray, six.binary_type)):
                data[key] = utils.text(value)
        for each in ('schedule', 'fetch', 'process', 'track'):
            if each in data:
                if data[each]:
                    data[each] = json.loads(data[each])
                else:
                    data[each] = {}
        return data

    def _stringify(self, data):
        for each in ('schedule', 'fetch', 'process', 'track'):
            if each in data:
                data[each] = json.dumps(data[each])
        return data

    def load_tasks(self, status, project=None, fields=None):
        if project and project not in self.projects:
            return
        where = "`status` = %s" % self.placeholder

        if project:
            projects = [project, ]
        else:
            projects = self.projects

        for project in projects:
            tablename = self._tablename(project)
            for each in self._select2dic(
                tablename, what=fields, where=where, where_values=(status, )
            ):
                yield self._parse(each)

    def get_task(self, project, taskid, fields=None):
        tablename = 'processdb'
        where = "`taskid` = %s and `project` = %s" % self.placeholder
        for each in self._select2dic(tablename, what=fields, where=where, where_values=(taskid, project)):
            return self._parse(each)
        return None

    def insert(self, project, taskid, group, process, fetch, url, obj={}):
        tablename = 'processdb'
        obj = dict(obj)
        obj['taskid'] = taskid
        obj['project'] = project
        if group is not None:
            obj['group'] = group
        obj['status'] = 1
        obj['process'] = json.dumps(process)
        if fetch is not None:
            obj['fetch'] = json.dumps(fetch)
        obj['url'] = url
        obj['scheduler_created_time'] = time.strftime('%Y-%m-%d %H:%M:%S')
        return self._insert(tablename, **self._stringify(obj))

    def update_status(self, project, taskid, status, fetcher_response_code=None, obj={}, **kwargs):
        tablename = 'processdb'
        obj = dict(obj)
        obj.update(kwargs)
        obj['status'] = status
        if fetcher_response_code is not None:
            obj['fetcher_response_code'] = fetcher_response_code
        if status == 2 or status ==3:
            obj['scheduler_to_fetcher_time'] = time.strftime('%Y-%m-%d %H:%M:%S')
        elif status == 11:
            obj['fetcher_begin_time'] = time.strftime('%Y-%m-%d %H:%M:%S')
        elif status == 12 or status == 13:
            obj['fetcher_end_time'] = time.strftime('%Y-%m-%d %H:%M:%S')
        elif status == 21:
            obj['processor_begin_time'] = time.strftime('%Y-%m-%d %H:%M:%S')
        elif status == 22 or status == 23:
            obj['processor_end_time'] = time.strftime('%Y-%m-%d %H:%M:%S')
        elif status == 31:
            obj['result_worder_begin_time'] = time.strftime('%Y-%m-%d %H:%M:%S')
        elif status == 32 or status == 33:
            obj['result_worder_end_time'] = time.strftime('%Y-%m-%d %H:%M:%S')
        return self._update(
            tablename,
            where="`taskid` = %s and `project` = %s",
            where_values=(taskid, project),
            **self._stringify(obj)
        )

    def clean(self, project):
        tablename = 'processdb'
        where = "`project` = %s" % self.placeholder
        return self._delete(tablename=tablename, where=where, where_values=(project, ))

