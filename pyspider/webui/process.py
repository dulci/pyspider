#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Binux<i@binux.me>
#         http://binux.me
# Created on 2014-10-19 16:23:55

from __future__ import unicode_literals

from flask import render_template, request, json
from flask import Response
from .app import app
from pyspider.libs import process_dump
import time


@app.route('/process')
def process():
    processdb = app.config['processdb']
    project = request.args.get('project')
    group = request.args.get('group')
    taskid = request.args.get('taskid')
    url = request.args.get('url')
    status = request.args.get('status')
    type = request.args.get('type', 3)
    offset = int(request.args.get('offset', 0))
    limit = int(request.args.get('limit', 20))

    count = processdb.count(project, group, taskid=taskid, url=url, status=status, type=type)
    results = list(processdb.select(project, taskid, group=group, url=url, status=status, type=type, offset=offset, limit=limit))
    common_fields = ('status', 'fetch', 'process', 'scheduler', 'fetcher', 'processor','result_worker')

    return render_template(
        "process.html", count=count, results=results, group=group,
        project=project, offset=offset, limit=limit, json=json,
        common_fields=common_fields, taskid='' if taskid is None else taskid,
        url='' if url is None else url, status='' if status is None else status,
        type='' if type is None else type
    )

@app.route('/restartwork')
def restart_allwork():
    processdb = app.config['processdb']
    # 根据task_id、url、type、status取task当前状态
    project = request.args.get('project')
    group = request.args.get('group')
    taskid = request.args.get('taskid')
    url = request.args.get('url')
    status = request.args.get('status')
    type = int(request.args.get('type', 3))
    task_results = list(processdb.select(project, taskid, group=group, url=url, status=status, type=type))
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
            if result['status'] != 32 and type == 3 and "placeholder.com" not in task['url'] and "placeholder-gcxx.com" not in task['url']:
                app.config['queues']['scheduler2fetcher'].put(task)
        return json.dumps({"status": "success"}), 200, {'Content-Type': 'application/json'}
    except Exception as e:
        return json.dumps({"error": e}), 404, {'Content-Type': 'application/json'}




@app.route('/processes/dump/<project>-<group>.<_format>')
def dump_processes(project, group, _format):
    resultdb = app.config['resultdb']

    offset = int(request.args.get('offset', 0)) or None
    limit = int(request.args.get('limit', 0)) or None
    results = resultdb.select(project, group, offset=offset, limit=limit)

    if _format == 'json':
        valid = request.args.get('style', 'rows') == 'full'
        return Response(process_dump.dump_as_json(results, valid),
                        mimetype='application/json')
    elif _format == 'txt':
        return Response(process_dump.dump_as_txt(results),
                        mimetype='text/plain')
    elif _format == 'csv':
        return Response(process_dump.dump_as_csv(results),
                        mimetype='text/csv')
