#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Binux<roy@binux.me>
#         http://binux.me
# Created on 2014-12-04 22:33:43

import re
import six
import time
import json
import sqlalchemy.exc

from sqlalchemy import (create_engine, MetaData, Table, Column, Index,
                        Integer, String, Float, LargeBinary, DateTime, func, and_, or_)
from sqlalchemy.engine.url import make_url
from pyspider.libs import utils
from pyspider.database.base.processdb import ProcessDB as BaseProcessDB
from .sqlalchemybase import SplitTableMixin, result2dict


class ProcessDB(SplitTableMixin, BaseProcessDB):
    __tablename__ = 'processdb'

    def __init__(self, url):
        self.table = Table('__tablename__', MetaData(),
                           Column('taskid', String(64), primary_key=True, nullable=False, index=True),
                           Column('project', String(64), primary_key=True, nullable=False, index=True),
                           Column('group', String(64)),
                           Column('type', Integer),
                           Column('status', Integer),
                           Column('process', String(2048)),
                           Column('fetch', String(2048)),
                           Column('fetcher_response_code', Integer),
                           Column('url', String(1024), index=True),
                           Column('scheduler_created_time', DateTime),
                           Column('scheduler_to_fetcher_time', DateTime),
                           Column('fetcher_begin_time', DateTime),
                           Column('fetcher_end_time', DateTime),
                           Column('processor_begin_time', DateTime),
                           Column('processor_end_time', DateTime),
                           Column('result_worder_begin_time', DateTime),
                           Column('result_worder_end_time', DateTime),
                           Column('created_at', DateTime),
                           Column('updated_at', DateTime),
                           mysql_engine='InnoDB',
                           mysql_charset='utf8'
                           )

        self.url = make_url(url)
        if self.url.database:
            database = self.url.database
            self.url.database = None
            try:
                engine = create_engine(self.url, convert_unicode=True, pool_recycle=3600)
                conn = engine.connect()
                conn.execute("commit")
                conn.execute("CREATE DATABASE %s" % database)
            except sqlalchemy.exc.SQLAlchemyError:
                pass
            self.url.database = database
        self.engine = create_engine(url, convert_unicode=True, pool_recycle=3600)

    def _create_project(self, project):
        assert re.match(r'^\w+$', project) is not None
        if project in self.projects:
            return
        self.table.name = self._tablename(project)
        Index('status_%s_index' % self.table.name, self.table.c.status)
        self.table.create(self.engine, checkfirst=True)
        self.table.indexes.clear()

    @staticmethod
    def _parse(data):
        for key, value in list(six.iteritems(data)):
            if isinstance(value, six.binary_type):
                data[key] = utils.text(value)
        for each in ('schedule', 'fetch', 'process', 'track'):
            if each in data:
                if data[each]:
                    if isinstance(data[each], bytearray):
                        data[each] = str(data[each])
                    data[each] = json.loads(data[each])
                else:
                    data[each] = {}
        return data

    @staticmethod
    def _stringify(data):
        for each in ('schedule', 'fetch', 'process', 'track'):
            if each in data:
                data[each] = utils.utf8(json.dumps(data[each]))
        return data

    def select(self, project, taskid, group=None, url=None, status=None, type=None, fields=None, offset=0, limit=None):
        self.table.name = self.__tablename__
        columns = [getattr(self.table.c, f, f) for f in fields] if fields else self.table.c
        whl_con = and_('1' == '1')
        # whl_con = and_(self.table.c.group == group)
        if taskid:
            whl_con = and_(whl_con, self.table.c.taskid == taskid)
        if project is not None and project != '':
            whl_con = and_(whl_con, self.table.c.project == project)
        if group is not None and group != '':
            whl_con = and_(whl_con,self.table.c.group == group)
        if url is not None and url != '':
            whl_con = and_(whl_con, self.table.c.url == url)
        if status is not None and status != '':
            status_con = None
            status_list = status.split(",")
            if len(status_list) > 1:
                for i in range(len(status_list)):
                    if i == 0:
                        status_con = or_(self.table.c.status == status_list[i])
                    else:
                        status_con = or_(status_con, self.table.c.status == status_list[i])
                whl_con = and_(whl_con, status_con)
            else:
                whl_con = and_(whl_con, self.table.c.status == status)
        if type is not None and type != 'all':
            whl_con = and_(whl_con, self.table.c.type == type)
        for process in self.engine.execute(self.table.select()
                                        .with_only_columns(columns=columns)
                                        .where(whl_con)
                                        .offset(offset).limit(limit)
                                        .execution_options(autocommit=True)):
            yield self._parse(result2dict(columns, process))

    def insert(self, project, taskid, group, process, fetch, url, obj={}):
        obj = dict(obj)
        obj['taskid'] = taskid
        obj['project'] = project
        if group is not None:
            obj['group'] = group
        obj['status'] = 1
        obj['process'] = process
        if taskid == 'on_start':
            obj['type'] = 1
        elif taskid == 'on_finished':
            obj['type'] = 4
        elif str(process).find('detail_page') != -1:
            obj['type'] = 3
        elif str(process).find('index_page') != -1:
            obj['type'] = 2
        else:
            obj['type'] = 9#过程页
        if fetch is not None:
            obj['fetch'] = fetch
        obj['url'] = url
        obj['scheduler_created_time'] = time.strftime('%Y-%m-%d %H:%M:%S')
        self.table.name = self.__tablename__
        return self.engine.execute(self.table.insert()
                                   .values(**self._stringify(obj)))

    def update_status(self, project, taskid, status, fetcher_response_code=None, obj={}, **kwargs):
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
        self.table.name = self.__tablename__
        return self.engine.execute(self.table.update()
                                   .where(and_(self.table.c.taskid == taskid, self.table.c.project == project))
                                   .values(**self._stringify(obj)))

    def update(self, project, taskid, obj={}, **kwargs):
        if project not in self.projects:
            self._list_project()
        if project not in self.projects:
            raise LookupError
        self.table.name = self.__tablename__
        obj = dict(obj)
        obj.update(kwargs)
        obj['updatetime'] = time.time()
        return self.engine.execute(self.table.update()
                                   .where(self.table.c.taskid == taskid)
                                   .values(**self._stringify(obj)))

    def count(self, project, group, taskid=None, url=None, status=None, type=None):
        self.table.name = self.__tablename__
        whl_con = and_('1' == '1')
        if project:
            whl_con = and_(whl_con, self.table.c.project == project)
        if group:
            whl_con = and_(whl_con,self.table.c.group == group)
        # whl_con = and_(self.table.c.project == project, self.table.c.group == group)
        if taskid is not None and taskid != '':
            whl_con = and_(whl_con, self.table.c.taskid == taskid)
        if url is not None and url != '':
            whl_con = and_(whl_con, self.table.c.url == url)
        if status is not None and status != '':
            status_con = None
            status_list = status.split(",")
            if len(status_list) > 1:
                for i in range(len(status_list)):
                    if i == 0:
                        status_con = or_(self.table.c.status == status_list[i])
                    else:
                        status_con = or_(status_con, self.table.c.status == status_list[i])
                whl_con = and_(whl_con, status_con)
            else:
                whl_con = and_(whl_con, self.table.c.status == status)
        if type is not None and type != 'all':
            whl_con = and_(whl_con, self.table.c.type == type)
        for count, in self.engine.execute(self.table.count()
                                                  .where(whl_con)):
            return count

    def clean(self, project):
        self.table.name = self.__tablename__
        return self.engine.execute(self.table.delete()
                                  .where(self.table.c.project == project))
