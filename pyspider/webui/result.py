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
from pyspider.libs import result_dump


@app.route('/results')
def result():
    resultdb = app.config['resultdb']
    project = request.args.get('project')
    group = request.args.get('group')
    offset = int(request.args.get('offset', 0))
    limit = int(request.args.get('limit', 20))

    count = resultdb.count(project, group)
    results = list(resultdb.select(project, group, offset=offset, limit=limit))

    return render_template(
        "result.html", count=count, results=results, group=group,
        result_formater=result_dump.result_formater,
        project=project, offset=offset, limit=limit, json=json
    )


@app.route('/results/dump/<project>-<group>.<_format>')
def dump_result(project, group,  _format):
    resultdb = app.config['resultdb']

    offset = int(request.args.get('offset', 0)) or None
    limit = int(request.args.get('limit', 0)) or None
    results = resultdb.select(project, group, offset=offset, limit=limit)

    if _format == 'json':
        valid = request.args.get('style', 'rows') == 'full'
        return Response(result_dump.dump_as_json(results, valid),
                        mimetype='application/json')
    elif _format == 'txt':
        return Response(result_dump.dump_as_txt(results),
                        mimetype='text/plain')
    elif _format == 'csv':
        return Response(result_dump.dump_as_csv(results),
                        mimetype='text/csv')
