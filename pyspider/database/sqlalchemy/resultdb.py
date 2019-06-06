#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Binux<roy@binux.me>
#         http://binux.me
# Created on 2014-12-04 18:48:15

import re
import six
import time
import json
import sqlalchemy.exc

from sqlalchemy import (create_engine, MetaData, Table, Column,
                        String, Float, LargeBinary, Integer, DateTime, and_, or_)
from sqlalchemy.engine.url import make_url
# from sqlalchemy.orm import sessionmaker
from pyspider.database.base.resultdb import ResultDB as BaseResultDB
from pyspider.libs import utils
from .sqlalchemybase import SplitTableMixin, result2dict

completion_delay_monitoring_record = Table('completion_delay_monitoring_record', MetaData(),
                        Column('id', Integer, primary_key=True, nullable=False),
                        Column('crawler_team_id', Integer),
                        Column('code', String(200)),
                        Column('website_type', Integer),
                        Column('title', String(200)),
                        Column('md5', String(50)),
                        Column('publish_date', DateTime),
                        Column('crawler_time', DateTime),
                        Column('upload_time', DateTime),
                        Column('upload_status', Integer),
                        Column('status', Integer),
                        Column('meddle_by', Integer),
                        Column('meddle_time', DateTime),
                        Column('meddle_remark', String(200)),
                        Column('is_del', Integer),
                        Column('deleted_at', DateTime),
                        Column('url', String(1024)),
                        Column('url_md5', String(50)),
                        Column('taskid', String(64)),
                        Column('skip_fetcher', Integer),
                        Column('use_proxy', Integer),
                        mysql_engine='InnoDB',
                        mysql_charset='utf8')

business_miss_monitoring_record = Table('business_miss_monitoring_record', MetaData(),
                        Column('id', Integer, primary_key=True, nullable=False),
                        Column('source', String(50)),
                        Column('url', String(500)),
                        Column('title', String(200)),
                        Column('publish_date', DateTime),
                        Column('crawler_time', DateTime),
                        Column('deal_status', Integer),
                        Column('remark', String(500)),
                        Column('deal_by', Integer),
                        Column('deal_time', DateTime),
                        Column('duty_person', String(255)),
                        Column('keyword', String(255)),
                        Column('website_url', String(500)),
                        Column('info_category', String(255)),
                        Column('is_del', Integer),
                        Column('find_flag', Integer),
                        Column('audit_status', Integer),
                        Column('assist_status', Integer),
                        Column('assist_url', String(500)),
                        mysql_engine='InnoDB',
                        mysql_charset='utf8')
class ResultDB(SplitTableMixin, BaseResultDB):
    __tablename__ = ''

    def __init__(self, url):
        self.table = Table('__tablename__', MetaData(),
                           Column('taskid', String(64), primary_key=True, nullable=False),
                           Column('project', String(100)),
                           Column('url', String(1024)),
                           Column('group', String(100)),
                           Column('result', LargeBinary),
                           Column('updatetime', Float(32)),
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
        self.engine = create_engine(url, convert_unicode=True,
                                    pool_recycle=3600)

        self._list_project()

    def _create_project(self, project):
        assert re.match(r'^\w+$', project) is not None
        if project in self.projects:
            return
        self.table.name = self._tablename(project)
        self.table.create(self.engine)

    @staticmethod
    def _parse(data):
        for key, value in list(six.iteritems(data)):
            if isinstance(value, six.binary_type):
                data[key] = utils.text(value)
        if 'result' in data:
            if isinstance(data['result'], bytearray):
                data['result'] = str(data['result'])
            data['result'] = json.loads(data['result'])
        return data

    @staticmethod
    def _stringify(data):
        if 'result' in data:
            data['result'] = utils.utf8(json.dumps(data['result']))
        return data

    def save(self, project, taskid, url, result, group=None):
        if group == 'self_crawler' or group == 'temp_crawler':
            tablename = "crawler_content_result_record"
        else:
            tablename = "crawler_result_record"
        self.table.name = self._tablename(tablename)
        obj = {
            'project': project,
            'taskid': taskid,
            'group': group,
            'url': url,
            'result': result,
            'updatetime': time.time(),
        }
        # if self.get(project, taskid, ('taskid', )):
        #     del obj['taskid']
        #     db_result = self.engine.execute(self.table.update()
        #                                .where(self.table.c.taskid == taskid)
        #                                .values(**self._stringify(obj)))
        # else:
        #     db_result = self.engine.execute(self.table.insert()
        #                                .values(**self._stringify(obj)))
        db_result = self._save(tablename, taskid, obj)
        if group == 'self_crawler' or group == 'temp_crawler':
            return db_result
        else:
            group_name = str(group).strip()
            if group_name == 'completion_delay_monitoring':
                # tablename = "completion_delay_monitoring_record"
                crawler_team_id = ""
                website_type = ""
                # Session = sessionmaker(bind=self.engine)
                # session = Session()
                try:
                    for one in self.engine.execute("select crawler_team_id, website_type from bid_website where code='%s'"%(project)):
                        crawler_team_id = one['crawler_team_id']
                        website_type = one['website_type']
                    obj = {
                        'crawler_team_id': crawler_team_id,
                        'code': project,
                        'website_type': website_type,
                        'title': str(result['title']).strip(),
                        'crawler_time': time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(time.time()))),
                        'taskid': taskid,
                        'url': url,
                    }
                    if result.get('publish_date'):
                        if len(result['publish_date']) <= 5:
                            publish_date = re.sub('[\[\]日]', '', re.sub('[年月/\\\]', '-', result['publish_date']))
                            targetMonth = int(time.strftime("%m", time.strptime(publish_date.replace("\\", "-"), "%m-%d")))
                            currentMonth = int(time.strftime("%m", time.localtime(time.time())))
                            if (currentMonth == 11 or currentMonth == 12) and (targetMonth == 1 or targetMonth == 2):
                                year = int(time.strftime("%Y", time.localtime(int(time.time())))) + 1
                            else:
                                year = int(time.strftime("%Y", time.localtime(int(time.time()))))
                            obj['publish_date'] = str(year) + "-" + publish_date
                        else:
                            obj['publish_date'] = re.sub('[\[\]日]', '', re.sub('[年月]', '-', result['publish_date']))
                    return self.other_table_save(completion_delay_monitoring_record, 'url', obj['url'], obj)
                except Exception as e:
                    pass
                    # session.close()
                finally:
                    pass
                    # session.close()
            elif group_name == 'business_miss_monitoring':
                # tablename = "business_miss_monitoring_record"
                crawler_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(time.time())))
                obj = {
                    'source': project,
                    'url': url,
                    'title': str(result['title']).strip(),
                    'publish_date': result['publish_date'],
                    'crawler_time': crawler_time,
                }
                return self.other_table_save(business_miss_monitoring_record, 'url', obj['url'], obj)

    def _save(self, project, taskid, fields):
        if self.get(project, taskid, ('taskid', ), project):
            del fields['taskid']
            db_result = self.engine.execute(self.table.update()
                                       .where(self.table.c.taskid == taskid)
                                       .values(**self._stringify(fields)))
        else:
            db_result = self.engine.execute(self.table.insert()
                                       .values(**self._stringify(fields)))
        return db_result

    def _get(self, table, column, column_value, fields=None):
        columns = [getattr(table.c, f, f) for f in fields] if fields else table.c
        for db_result in self.engine.execute(table.select()
                                        .with_only_columns(columns=columns)
                                        .where(getattr(table.c, column) == column_value)
                                        .limit(1)):
            return self._parse(result2dict(columns, db_result))

    def other_table_save(self, table, column, column_value, fields):
        if self._get(table, column, column_value, (column, )):
            del fields['column']
            db_result = self.engine.execute(table.update()
                                       .where(getattr(table.c, column) == column_value)
                                       .values(**self._stringify(fields)))
        else:
            db_result = self.engine.execute(table.insert()
                                       .values(**self._stringify(fields)))
        return db_result

    def select(self, project, fields=None, offset=0, limit=None):
        if project not in self.projects:
            self._list_project()
        if project not in self.projects:
            return
        self.table.name = self._tablename(project)

        columns = [getattr(self.table.c, f, f) for f in fields] if fields else self.table.c
        for task in self.engine.execute(self.table.select()
                                        .with_only_columns(columns=columns)
                                        .order_by(self.table.c.updatetime.desc())
                                        .offset(offset).limit(limit)
                                        .execution_options(autocommit=True)):
            yield self._parse(result2dict(columns, task))

    def select(self, project, group, taskid=None, url=None, upload_status=None, fields=None, offset=0, limit=None):
        if group == 'self_crawler' or group == 'temp_crawler':
            tablename = "crawler_content_result_record"
        elif group == 'completion_delay_monitoring':
            tablename = "crawler_result_record"
        self.table.name = self._tablename(tablename)
        whl_con = and_(self.table.c.project == project)
        if taskid:
            whl_con = and_(whl_con, self.table.c.taskid == taskid)
        if url:
            whl_con = and_(whl_con, self.table.c.url == url)
        if upload_status:
            status_con = None
            for i in upload_status.split(","):
                status_con = or_(status_con, self.table.c.upload_status == i) if status_con else or_(self.table.c.upload_status == i)
            whl_con = and_(whl_con, status_con)
        columns = [getattr(self.table.c, f, f) for f in fields] if fields else self.table.c
        for task in self.engine.execute(self.table.select()
                                                .with_only_columns(columns=columns)
                                                .where(whl_con)
                                                .order_by(self.table.c.updatetime.desc())
                                                .offset(offset).limit(limit)
                                                .execution_options(autocommit=True)):
            yield self._parse(result2dict(columns, task))

    def count(self, project):
        if project not in self.projects:
            self._list_project()
        if project not in self.projects:
            return 0
        self.table.name = self._tablename(project)

        for count, in self.engine.execute(self.table.count()):
            return count

    def count(self, project, group, taskid=None, url=None, upload_status=None):
        if group == 'self_crawler' or group == 'temp_crawler':
            tablename = "crawler_content_result_record"
        elif group == 'completion_delay_monitoring':
            tablename = "crawler_result_record"
        self.table.name = self._tablename(tablename)
        whl_con = and_(self.table.c.project == project)
        if taskid:
            whl_con = and_(whl_con, self.table.c.taskid == taskid)
        if url:
            whl_con = and_(whl_con, self.table.c.url == url)
        if upload_status:
            status_con = None
            for i in upload_status.split(","):
                status_con = or_(status_con, self.table.c.upload_status == i) if status_con else or_(
                    self.table.c.upload_status == i)
            whl_con = and_(whl_con, status_con)
        for count, in self.engine.execute(self.table.count().where(whl_con)):
            return count

    def get(self, project, taskid, fields=None, table_name='crawler_result_record'):
        # if project not in self.projects:
        #     self._list_project()
        # if project not in self.projects:
        #     return

        self.table.name = self._tablename(table_name)

        columns = [getattr(self.table.c, f, f) for f in fields] if fields else self.table.c
        for task in self.engine.execute(self.table.select()
                                        .with_only_columns(columns=columns)
                                        .where(and_(self.table.c.taskid == taskid, self.table.c.project == project))
                                        .limit(1)):
            return self._parse(result2dict(columns, task))

    def get_content(self, project, taskid, fields={'taskid', 'project', 'url'}, table_name='crawler_content_result_record'):
        self.table.name = self._tablename(table_name)
        columns = [getattr(self.table.c, f, f) for f in fields] if fields else self.table.c
        try:
            for task in self.engine.execute(self.table.select()
                                                .with_only_columns(columns=columns)
                                                .where(
                                                and_(self.table.c.taskid == taskid,
                                                self.table.c.project == project))):
                return self._parse(result2dict(columns, task))
        except:
            return None

    def clean(self, project, group):
        if group == 'self_crawler' or group == 'temp_crawler':
            self.table.name = "crawler_content_result_record"
            return self.engine.execute(self.table.delete()
                                       .where(self.table.c.project == project))
        elif group == 'completion_delay_monitoring':
            self.table.name = "crawler_result_record"
            return self.engine.execute(self.table.delete()
                                       .where(self.table.c.project == project))
            return self.engine.execute(completion_delay_monitoring_record.delete()
                                       .where(self.table.c.project == project))