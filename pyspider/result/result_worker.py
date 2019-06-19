#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Binux<i@binux.me>
#         http://binux.me
# Created on 2014-10-19 15:37:46
import sys
import os
sys.path.append( os.path.join( os.path.abspath(os.path.dirname(__file__)) , '../'))
import time
import json
import logging
import datetime
from six.moves import queue as Queue
from pyspider.libs.utils import md5string
import re
logger = logging.getLogger("result")


class ResultWorker(object):

    """
    do with result
    override this if needed.
    """

    def __init__(self, taskdb, resultdb, inqueue, content_queue, processdb=None, projectcache=None):
        self.taskdb = taskdb
        self.resultdb = resultdb
        self.processdb = processdb
        self.projectcache = projectcache
        self.inqueue = inqueue
        self._quit = False
        self.content_queue = content_queue

    def on_result(self, task, result):
        # repeat check
        if task['group'] is not None and task['group'] != 'self_crawler':
            oldTask = self.resultdb.get(task['project'], task['taskid'])
            if oldTask is not None:
                logger.info('result %s:%s %s is already existed'%(task['project'], task['taskid'], task['url']))
                return
        elif task['group'] is not None and (task['group'] == 'self_crawler' or task['group'] == 'temp_crawler'):
            oldTask = self.resultdb.get_content(task['project'], task['taskid'])
            if oldTask is not None:
                logger.info('result %s:%s %s is already existed'%(task['project'], task['taskid'], task['url']))
                if self.processdb is not None:
                    self.processdb.update_status(project=task['project'], taskid=task['taskid'], status=32)
                return

        # reset project delay level
        if self.projectcache is not None:
            self.projectcache.set_project_delay_level(task['project'], 0)
        '''Called every result'''
        if not result:
            if self.processdb is not None:
                self.processdb.update_status(project=task['project'], taskid=task['taskid'], status=34)
            return
        if 'taskid' in task and 'project' in task and 'url' in task:
            try:
                logger.info('result %s:%s %s -> %.30r' % (
                    task['project'], task['taskid'], task['url'], result))
                exeUrl = re.sub(r';jsessionid=[0-9A-Za-z]{1,32}(\.server\d)?', '', task['url'])
                exeUrl = re.sub(r'\?pa=[0-9]{0,8}$', '', exeUrl)
                result_content = dict()
                if task['group'] == 'self_crawler':
                    result_content['taskid'] = task['taskid']
                    result_content['ddid'] = md5string(result['html'])
                    result_content['type'] = task['project']
                    result_content['html'] = str(result['html'])
                    result_content['jhycontent'] = str(result['jhycontent'])
                    result_content['currentTime'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    result_content['publishTime'] = result['publishTime']
                    result_content['link'] = exeUrl
                    result_content['jhytitle'] = result['title']
                    result_content['taskName'] = result['taskName']
                    result_content['contentTitle'] = result['contentTitle']
                    result_content['crawlerTeamId'] = result['crawlerTeamId']
                    result = result_content
                if task['group'] != 'self_crawler':
                    if result["title"] == None or result["title"].strip() == '':
                        return
                res = self.resultdb.save(
                    project=task['project'],
                    taskid=task['taskid'],
                    url=exeUrl,
                    result=result,
                    group=task['group'])
                if task['group'] == 'self_crawler':
                    result_content_str = json.dumps(result_content)
                    self.content_queue.put(result_content_str)
                if self.processdb is not None:
                    self.processdb.update_status(project=task['project'], taskid=task['taskid'], status=32)
                return res
            except Exception as e:
                print(task)
                print(result)
                logger.exception(e)
                if self.processdb is not None:
                    self.processdb.update_status(project=task['project'], taskid=task['taskid'], status=35)
                logger.warning('result svae failure -> %.30r' % e)
                return
        else:
            if self.processdb is not None:
                self.processdb.update_status(project=task['project'], taskid=task['taskid'], status=33)
            logger.warning('result UNKNOW -> %.30r' % result)
            return

    def quit(self):
        self._quit = True

    def run(self):
        '''Run loop'''
        logger.info("result_worker starting...")

        while not self._quit:
            try:
                task, result = self.inqueue.get(timeout=1)
                if self.processdb is not None:
                    self.processdb.update_status(project=task['project'], taskid=task['taskid'], status=31)
                self.on_result(task, result)
            except Queue.Empty as e:
                continue
            except KeyboardInterrupt:
                break
            except AssertionError as e:
                logger.error(e)
                continue
            except Exception as e:
                logger.error(task['project'] + ":" + task['taskid'] + ":" + task['url'])
                logger.exception(e)
                continue

        logger.info("result_worker exiting...")


class OneResultWorker(ResultWorker):
    '''Result Worker for one mode, write results to stdout'''
    def on_result(self, task, result):
        '''Called every result'''
        if not result:
            return
        if 'taskid' in task and 'project' in task and 'url' in task:
            logger.info('result %s:%s %s -> %.30r' % (
                task['project'], task['taskid'], task['url'], result))
            print(json.dumps({
                'taskid': task['taskid'],
                'project': task['project'],
                'url': task['url'],
                'result': result,
                'updatetime': time.time()
            }))
        else:
            logger.warning('result UNKNOW -> %.30r' % result)
            return
