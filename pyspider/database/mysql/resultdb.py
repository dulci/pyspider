#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Binux<i@binux.me>
#         http://binux.me
# Created on 2014-10-13 22:02:57

import re
import six
import time
import json
import mysql.connector
import hashlib

from pyspider.libs import utils
from pyspider.database.base.resultdb import ResultDB as BaseResultDB
from pyspider.database.basedb import BaseDB
from .mysqlbase import MySQLMixin, SplitTableMixin


class ResultDB(MySQLMixin, SplitTableMixin, BaseResultDB, BaseDB):
    __tablename__ = ''

    def __init__(self, host='localhost', port=3306, database='resultdb',
                 user='root', passwd=None):
        self.database_name = database
        self.conn = mysql.connector.connect(user=user, password=passwd,
                                            host=host, port=port, autocommit=True)
        if database not in [x[0] for x in self._execute('show databases')]:
            self._execute('CREATE DATABASE %s' % self.escape(database))
        self.conn.database = database
        self._list_project()

    def _create_project(self, project):
        assert re.match(r'^\w+$', project) is not None
        tablename = self._tablename(project)
        if tablename in [x[0] for x in self._execute('show tables')]:
            return
        self._execute('''CREATE TABLE %s (
            `taskid` varchar(64) PRIMARY KEY,
            `url` varchar(1024),
            `result` MEDIUMBLOB,
            `updatetime` double(16, 4),
            `skip_fetcher` int(11),
            `use_proxy` int(11)
            ) ENGINE=InnoDB CHARSET=utf8''' % self.escape(tablename))

    def _parse(self, data):
        for key, value in list(six.iteritems(data)):
            if isinstance(value, (bytearray, six.binary_type)):
                data[key] = utils.text(value)
        if 'result' in data:
            data['result'] = json.loads(data['result'])
        return data

    def _stringify(self, data):
        if 'result' in data:
            data['result'] = json.dumps(data['result'])
        return data

    def save(self, project, taskid, url, result, group):
        if group == 'self_crawler':
            # 内容记录表保存
            tablename = "crawler_content_result_record"
            if project not in self.projects:
                # self._create_project(project)
                self._list_project()
            # 默认存放站源完整性表中
            group_name = str(group).strip()
            obj = {
                'project': project,
                'taskid': taskid,
                'group': group,
                'url': url,
                'result': result,
                'updatetime': time.time(),
            }
            return self._replace(tablename, **self._stringify(obj))
        else:
            # 记录表保存
            tablename = "crawler_result_record"
            if project not in self.projects:
                #self._create_project(project)
                self._list_project()
            # 默认存放站源完整性表中
            group_name = str(group).strip()
            obj = {
                'project': project,
                'taskid': taskid,
                'group': group,
                'url': url,
                'result': result,
                'updatetime': time.time(),
            }
            self._replace(tablename, **self._stringify(obj))
        if group_name == 'completion_delay_monitoring':
            # 及时性完整性监控表保存
            tablename = "completion_delay_monitoring_record"
            # get team_id
            crawler_team_id = ""
            website_type = ""
            team_id_res = self._select("bid_website", 'crawler_team_id, website_type', "code = %s", [project], 0, 1)
            for res in team_id_res:
                if res:
                    crawler_team_id = res[0]
                    website_type = res[1]
                    break
            crawler_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(time.time())))
            obj = {
                'crawler_team_id': crawler_team_id,
                'code': project,
                'website_type': website_type,
                'title': str(result['title']).strip(),
                'crawler_time': crawler_time,
                'taskid': taskid,
                'url': url,
            }
            if 'publish_date' in result and str(result['publish_date']) != '':
                obj['publish_date'] = str(result['publish_date']).replace("年", "-").replace("月", "-").replace("日", "").replace("[", "").replace("]", "")
            if 'publish_date' in result and str(result['publish_date']) != '' and len(result['publish_date']) <= 5:
                publish_date = str(result['publish_date']).replace("年", "-").replace("月", "-").replace("日", "").replace("[", "").replace("]", "").replace("\\", "-").replace("/", "-")
                targetMonth = int(time.strftime("%m", time.strptime(publish_date.replace("\\", "-"), "%m-%d")))
                currentMonth = int(time.strftime("%m", time.localtime(int(time.time()))))
                if (currentMonth == 11 or currentMonth == 12) and (targetMonth == 1 or targetMonth == 2):
                    year = int(time.strftime("%Y", time.localtime(int(time.time())))) + 1
                else:
                    year = int(time.strftime("%Y", time.localtime(int(time.time()))))
                obj['publish_date'] = str(year) + "-" + publish_date
            return self._replace(tablename, **self._stringify(obj))
        elif group_name == 'business_miss_monitoring':
            # 行业监控表保存
            tablename = "business_miss_monitoring_record"
            crawler_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(time.time())))
            obj = {
                'source': project,
                'url': url,
                'title': str(result['title']).strip(),
                'publish_date': result['publish_date'],
                'crawler_time': crawler_time,
            }
            return self._replace(tablename, **self._stringify(obj))

    def select(self, project, group, fields=None, offset=0, limit=None):
        # 去掉project存在性校验
        # if project not in self.projects:
        #     self._list_project()
        # if project not in self.projects:
        #     return
        # tablename = self._tablename(project)
        if group == 'self_crawler':
            tablename = "crawler_content_result_record"
        elif group == 'completion_delay_monitoring':
            tablename = "crawler_result_record"

        for task in self._select2dic(tablename, where="project = %s", where_values=[project], what=fields, order='updatetime DESC',
                                     offset=offset, limit=limit):
            yield self._parse(task)

    def select(self, project, group, taskid=None, url=None, upload_status=None, fields=None, offset=0, limit=None):
        if group == 'self_crawler':
            tablename = "crawler_content_result_record"
        elif group == 'completion_delay_monitoring':
            tablename = "crawler_result_record"

        where = "project = %s" + \
                (" and `taskid` = %s" if taskid is not None and taskid != '' else "") + \
                (" and `url` = %s" if url is not None and url != '' else "")
        if upload_status is not None and upload_status != '':
            upload_status_condition = ""
            for i in upload_status.split(","):
                upload_status_condition = upload_status_condition + " or `upload_status` = %s"
            where = where + " and (" + upload_status_condition[3:] + ")"
        where_values = list()
        where_values.append(project)
        if taskid is not None and taskid != '':
            where_values.append(taskid)
        if url is not None and url != '':
            where_values.append(url)
        if upload_status is not None and len(upload_status) > 0:
            where_values.extend(upload_status.split(","))
        for task in self._select2dic(tablename, where=where, where_values=where_values, what=fields, order='updatetime DESC',
                                     offset=offset, limit=limit):
            yield self._parse(task)

    def count(self, project, group):
        if group == 'self_crawler':
            tablename = "crawler_content_result_record"
        elif group == 'completion_delay_monitoring':
            tablename = "crawler_result_record"
        for count, in self._execute("SELECT count(1) FROM %s WHERE `group` = '%s' and `project` = '%s'" % (self.escape(tablename), group, project)):
            return count

    def count(self, project, group, taskid=None, url=None, upload_status=None):
        if group == 'self_crawler':
            tablename = "crawler_content_result_record"
        elif group == 'completion_delay_monitoring':
            tablename = "crawler_result_record"
        count_sql = "SELECT count(1) FROM %s WHERE `group` = '%s' and `project` = '%s'" + (" and `taskid` = '%s'" if taskid is not None and taskid != '' else "") \
                    + (" and `url` = '%s'" if url is not None and url != '' else "")
        if upload_status is not None and upload_status != '':
            upload_status_condition = ""
            for i in upload_status.split(","):
                upload_status_condition = upload_status_condition + " or `upload_status` = '%s'"
            upload_status_condition = " and (" + upload_status_condition[3:] + ")"
            count_sql = count_sql + upload_status_condition
        count_sql_arg = [self.escape(tablename), group, project]
        if taskid is not None and taskid != '':
            count_sql_arg.append(taskid)
        if url is not None and url != '':
            count_sql_arg.append(url)
        if upload_status is not None and upload_status != '':
            count_sql_arg.extend(upload_status.split(","))
        for count, in self._execute(count_sql % tuple(count_sql_arg)):
            return count


    def get(self, project, taskid, fields=None):
        # if project not in self.projects:
        #     self._list_project()
        # if project not in self.projects:
        #     return
        tablename = "crawler_result_record"
        where = "`taskid` = %s and project = %s"
        for task in self._select2dic(tablename, what=fields,
                                     where=where, where_values=(taskid, project,)):
            return self._parse(task)

    def get_content(self, project, taskid, fields=None):
        tablename = "crawler_content_result_record"
        where = "`taskid` = %s and project = %s"
        for task in self._select2dic(tablename, what=fields,
                                     where=where, where_values=(taskid, project,)):
            return self._parse(task)

    def clean(self, project, group):
        if group == 'self_crawler':
            tablename = "crawler_content_result_record"
            self._delete(tablename, where="project = %s", where_values=[project])
        elif group == 'completion_delay_monitoring':
            tablename = "crawler_result_record"
            self._delete(tablename, where="project = %s", where_values=[project])
            tablename = "completion_delay_monitoring_record"
            self._delete(tablename, where="code = %s", where_values=[project])
