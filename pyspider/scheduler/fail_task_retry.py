#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import time
import math
import logging

logger = logging.getLogger('scavenger')
class FailTaskRetry(object):
    RETRY_FAIL_TASK_INTERVAL = 30 * 60
    def __init__(self, processdb, queues, groups=['self_crawler']):
        self._last_retry_fail_task_time = 0
        self.processdb = processdb
        self.groups = groups
        self.queues = queues

    def run(self):
        logger.info("scavenger starting...")
        while True:
            self.retry_fail_task()
            logger.info('retry failed task once')
            time.sleep(self.RETRY_FAIL_TASK_INTERVAL)

    def retry_fail_task(self):
        if self._last_retry_fail_task_time and self._last_retry_fail_task_time + self.RETRY_FAIL_TASK_INTERVAL > time.time():
            return
        fail_status = '3,13,14,23,33,34,35'
        suspected_fail_status = '1,2,4,11,12,15,21,22,31'
        limit = 1000
        for group in self.groups:
            fail_count = self.processdb.count(None, group, status=fail_status, type=3)
            logger.info('fail task num is %d'%(fail_count))
            pages = math.ceil(fail_count/limit)
            for page in range(pages):
                out_queue = sorted(self.queues, key=lambda x: x.qsize())[0]
                fail_task_results = list(self.processdb.select(None, None, group=group, url='', status=fail_status, type=3, limit=limit, offset=page*limit))
                for fail_task in fail_task_results:
                    task = self._put_fail_task_agin(fail_task)
                    if task:
                        out_queue.put(task)
                        logger.info('fail task:%s:%s is restart' % (task['project'], task['taskid']))
            suspected_fail_count = self.processdb.count(None, group, status=suspected_fail_status, type=3)
            logger.info('suspected fail task num is %d' % (suspected_fail_count))
            pages = math.ceil(suspected_fail_count/limit)
            for page in range(pages):
                out_queue = sorted(self.queues, key=lambda x: x.qsize())[0]
                suspected_fail_results = list(self.processdb.select(None, None, group=group, url='', status=suspected_fail_status, type=3, limit=limit, offset=page*limit))
                for suspected_fail_task in suspected_fail_results:
                    task = self._put_fail_task_agin(suspected_fail_task)
                    if task:
                        out_queue.put(task)
                        logger.info('out of 10 minute not execute the next step task:%s:%s is restart' % (task['project'], task['taskid']))
        self._last_retry_fail_task_time = time.time()

    def _put_fail_task_agin(self, source_task, reset_keys=['taskid','url','project','fetch','process','group']):
        task = dict()
        # if source_task['status'] == 1 and (time.mktime(source_task['updated_at'].timetuple()) - time.mktime(source_task['created_at'].timetuple())) < 600:
        #     return
        # if source_task['status'] == 11 and (time.time() - time.mktime(source_task['fetcher_begin_time'].timetuple())) < 600:
        #     return
        # if source_task['status'] == 21 and (time.time() - time.mktime(source_task['processor_begin_time'].timetuple())) < 600:
        #     return
        # if source_task['status'] == 31 and (time.time() - time.mktime(source_task['result_worder_begin_time'].timetuple())) < 600:
        #     return
        if (time.time() - time.mktime(source_task['scheduler_created_time'].timetuple())) < self.RETRY_FAIL_TASK_INTERVAL:
            return
        for key in reset_keys:
            task[key] = source_task.get(key)
        schedule = {'force_update': True, 'age': 0}
        task['schedule'] = schedule
        task['status'] = 1
        task['track'] = {}
        task['lastcrawltime'] = None
        task['type'] = 1
        task['project_updatetime'] = time.time()
        return task
