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
    results = list(processdb.select(project, group, taskid=taskid, url=url, status=status, type=type, offset=offset, limit=limit))
    common_fields = ('status', 'fetch', 'process', 'scheduler', 'fetcher', 'processor','result_worker')

    return render_template(
        "process.html", count=count, results=results, group=group,
        project=project, offset=offset, limit=limit, json=json,
        common_fields=common_fields, taskid='' if taskid is None else taskid,
        url='' if url is None else url, status='' if status is None else status,
        type='' if type is None else type
    )


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
