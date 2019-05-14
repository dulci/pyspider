#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Binux<i@binux.me>
#         http://binux.me
# Created on 2014-02-22 23:20:39

import socket
import math
import time

from six import iteritems, itervalues
from flask import render_template, request, json

try:
    import flask_login as login
except ImportError:
    from flask.ext import login

from .app import app

index_fields = ['name', 'group', 'status', 'comments', 'rate', 'burst', 'updatetime', 'remark']


@app.route('/', methods=['GET', 'POST'])
def index():
    group = 'all'
    name = None
    status = 'ALL'
    page = 1
    pageSize = 20
    if request.form:
        group = request.form['group']
        name = request.form['name']
        status = request.form['status']
        page = request.form['page']
        pageSize = request.form['pageSize']
    search_condition = {}
    search_condition["group"] = group
    if name:
        search_condition["name"] = name
    search_condition["status"] = status
    search_condition['page'] = page
    search_condition['pageSize'] = pageSize
    projectdb = app.config['projectdb']
    projects = sorted(projectdb.get_all(fields=index_fields, search_condition=search_condition),
                      key=lambda k: (0 if k['group'] else 1, k['group'] or '', k['name']))
    count = projectdb.count(search_condition)
    pages = math.ceil(int(count)/int(pageSize))
    return render_template("index.html", projects=projects, group=group, name=name, status=status, count=count, page=page, pageSize=pageSize, pages=pages)


@app.route('/queues')
def get_queues():
    def try_get_qsize(queue):
        if queue is None:
            return 'None'
        try:
            return queue.qsize()
        except Exception as e:
            return "%r" % e

    result = {}
    queues = app.config.get('queues', {})
    for key in queues:
        result[key] = try_get_qsize(queues[key])
    return json.dumps(result), 200, {'Content-Type': 'application/json'}

@app.route('/update_remark', methods=['POST', ])
def remark_update():
    projectdb = app.config['projectdb']
    project = request.form['pk']
    name = request.form['name']
    remark = request.form['value']

    project_info = projectdb.get(project, fields=('name', 'group'))
    if not project_info:
        return "no such project.", 404
    if 'lock' in projectdb.split_group(project_info.get('group')) \
            and not login.current_user.is_active():
        return app.login_response
    try:
        update = {name : remark}
        projectdb.update(project, update)
        return 'ok', 200
    except Exception as e:
        return e, 500




@app.route('/update', methods=['POST', ])
def project_update():
    projectdb = app.config['projectdb']
    project = request.form['pk']
    name = request.form['name']
    value = request.form['value']

    project_info = projectdb.get(project, fields=('name', 'group'))
    if not project_info:
        return "no such project.", 404
    if 'lock' in projectdb.split_group(project_info.get('group')) \
            and not login.current_user.is_active():
        return app.login_response

    if name not in ('group', 'status', 'rate'):
        return 'unknown field: %s' % name, 400
    if name == 'rate':
        value = value.split('/')
        if len(value) != 2:
            return 'format error: rate/burst', 400
        rate = float(value[0])
        burst = float(value[1])
        update = {
            'rate': min(rate, app.config.get('max_rate', rate)),
            'burst': min(burst, app.config.get('max_burst', burst)),
        }
    else:
        update = {
            name: value
        }

    ret = projectdb.update(project, update)
    if ret:
        rpc = app.config['scheduler_rpc']
        if rpc is not None:
            try:
                rpc.update_project()
            except socket.error as e:
                app.logger.warning('connect to scheduler rpc error: %r', e)
                return 'rpc error', 200
        return 'ok', 200
    else:
        return 'update error', 500


@app.route('/counter')
def counter():
    projects = str(request.values.get('projects')).split(",")
    rpc = app.config['scheduler_rpc']
    if rpc is None:
        return json.dumps({})

    result = {}
    try:
        data = rpc.webui_update(projects)
        if data.get('counter') is not None:
            for type, counters in iteritems(data['counter']):
                if counters is not None:
                    for project, counter in iteritems(counters):
                        result.setdefault(project, {})[type] = counter
        if data.get('pause_status') is not None:
            for project, paused in iteritems(data['pause_status']):
                result.setdefault(project, {})['paused'] = paused
    except socket.error as e:
        app.logger.warning('connect to scheduler rpc error: %r', e)
        return json.dumps({}), 200, {'Content-Type': 'application/json'}

    return json.dumps(result), 200, {'Content-Type': 'application/json'}


@app.route('/run', methods=['POST', ])
def runtask():
    rpc = app.config['scheduler_rpc']
    if rpc is None:
        return json.dumps({})

    projectdb = app.config['projectdb']
    project = request.form['project']
    project_info = projectdb.get(project, fields=('name', 'group'))
    if not project_info:
        return "no such project.", 404
    if 'lock' in projectdb.split_group(project_info.get('group')) \
            and not login.current_user.is_active():
        return app.login_response

    newtask = {
        "project": project,
        "taskid": "on_start",
        "url": "data:,on_start",
        "process": {
            "callback": "on_start",
        },
        "schedule": {
            "age": 0,
            "priority": 9,
            "force_update": True,
        },
    }

    try:
        ret = rpc.newtask(newtask)
    except socket.error as e:
        app.logger.warning('connect to scheduler rpc error: %r', e)
        return json.dumps({"result": False}), 200, {'Content-Type': 'application/json'}
    return json.dumps({"result": ret}), 200, {'Content-Type': 'application/json'}


@app.route('/restartallwork', methods=['POST', ])
def restartallword():
    processdb = app.config['processdb']
    # 根据task_id、url、type、status取task当前状态
    project = request.args.get('project')
    group = request.args.get('group', 'self_crawler')
    status = "1,3,11,13,14,21,23,31,33,34,35"
    task_results = list(processdb.select(project, group, taskid='', url='', status=status, type=3))
    try:
        for result in task_results:
            task = dict()
            task['taskid'] = result['taskid']
            task['url'] = result['url']
            task['project'] = result['project']
            task['fetch'] = result['fetch']
            task['process'] = result['process']
            task['group'] = result['group']
            schedule = {'force_update': True, 'age': 0}
            task['schedule'] = schedule
            task['status'] = 1
            task['track'] = {}
            task['lastcrawltime'] = None
            task['type'] = 1
            task['project_updatetime'] = time.time()
            if result['status'] != 32 and "placeholder.com" not in task['url'] and "placeholder-gcxx.com" not in task['url']:
                app.config['queues']['scheduler2fetcher'].put(task)
        return json.dumps({"status": "success"}), 200, {'Content-Type': 'application/json'}
    except Exception as e:
        return json.dumps({"error": e}), 404, {'Content-Type': 'application/json'}


@app.route('/clean', methods=['POST', ])
def clean():
    taskdb = app.config['taskdb']
    resultdb = app.config['resultdb']
    processdb = app.config['processdb']
    project = request.form['project']
    group = request.form['group']
    if processdb is not None:
        processdb.clean(project)
    taskdb.clean(project)
    resultdb.clean(project, group)
    return json.dumps({"result": True}), 200, {'Content-Type': 'application/json'}

@app.route('/robots.txt')
def robots():
    return """User-agent: *
Disallow: /
Allow: /$
Allow: /debug
Disallow: /debug/*?taskid=*
""", 200, {'Content-Type': 'text/plain'}
