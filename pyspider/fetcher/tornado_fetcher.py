#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Binux<i@binux.me>
#         http://binux.me
# Created on 2012-12-17 11:07:19

from __future__ import unicode_literals

import os
import sys
import six
import copy
import json
import logging
import traceback
import functools
import threading
import tornado.ioloop
import tornado.httputil
import tornado.httpclient
import pyspider
import re

from six.moves import queue, http_cookies
from six.moves.urllib.robotparser import RobotFileParser
from requests import cookies
from six.moves.urllib.parse import urljoin, urlsplit
from tornado import gen
from tornado.curl_httpclient import CurlAsyncHTTPClient
from tornado.simple_httpclient import SimpleAsyncHTTPClient

from pyspider.libs import utils, dataurl, counter
from pyspider.libs.url import quote_chinese
from .cookie_utils import extract_cookies_to_jar

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver import ActionChains
from pyspider.libs.web_drivers import WebDrivers
import time
logger = logging.getLogger('fetcher')


class MyCurlAsyncHTTPClient(CurlAsyncHTTPClient):

    def free_size(self):
        return len(self._free_list)

    def size(self):
        return len(self._curls) - self.free_size()


class MySimpleAsyncHTTPClient(SimpleAsyncHTTPClient):

    def free_size(self):
        return self.max_clients - self.size()

    def size(self):
        return len(self.active)

fetcher_output = {
    "status_code": int,
    "orig_url": str,
    "url": str,
    "headers": dict,
    "content": str,
    "cookies": dict,
}


class Fetcher(object):
    user_agent = "pyspider/%s (+http://pyspider.org/)" % pyspider.__version__
    default_options = {
        'method': 'GET',
        'headers': {
        },
        'use_gzip': True,
        'timeout': 60,
        'connect_timeout': 70,
    }
    phantomjs_proxy = None
    splash_endpoint = None
    splash_lua_source = open(os.path.join(os.path.dirname(__file__), "splash_fetcher.lua")).read()
    robot_txt_age = 60*60  # 1h
    proxy_retry_times = 3

    def __init__(self, inqueue, outqueue, poolsize=100, proxy=None, proxypooldb=None, lifetime=None, proxyname=None, proxyparam=None, async_mode=True, configure=None, processdb=None, taskdb=None, fetcherrorprojectdb=None):
        self.inqueue = inqueue
        self.outqueue = outqueue

        self.processdb = processdb
        self.taskdb = taskdb
        self.fetcherrorprojectdb = fetcherrorprojectdb

        self.poolsize = poolsize
        self._running = False
        self._quit = False
        self.proxy = proxy
        self.proxypooldb = proxypooldb
        self.lifetime = lifetime
        self.proxyname = proxyname
        self.proxyparam = proxyparam
        self.async_mode = async_mode
        self.ioloop = tornado.ioloop.IOLoop()
        self.drivers = WebDrivers()
        self.configure = configure

        self.robots_txt_cache = {}

        # binding io_loop to http_client here
        if self.async_mode:
            self.http_client = MyCurlAsyncHTTPClient(max_clients=self.poolsize,
                                                     io_loop=self.ioloop)
        else:
            self.http_client = tornado.httpclient.HTTPClient(MyCurlAsyncHTTPClient, max_clients=self.poolsize)

        self._cnt = {
            '5m': counter.CounterManager(
                lambda: counter.TimebaseAverageWindowCounter(30, 10)),
            '1h': counter.CounterManager(
                lambda: counter.TimebaseAverageWindowCounter(60, 60)),
        }

        if proxypooldb is not None and lifetime is not None and proxyname is not None:
            from .proxy_pool import ProxyPool
            self.proxypool = ProxyPool(proxypooldb=proxypooldb, lifetime=lifetime, proxyname=proxyname, proxyparam=proxyparam)

    def send_result(self, type, task, result):
        '''Send fetch result to processor'''
        if self.outqueue:
            try:
                self.outqueue.put((task, result))
                if self.processdb is not None:
                    self.processdb.update_status(project=task['project'], taskid=task['taskid'], fetcher_response_code=result['status_code'], status=12)
            except Exception as e:
                logger.exception(e)
                if self.processdb is not None:
                    self.processdb.update_status(project=task['project'], taskid=task['taskid'], fetcher_response_code=result['status_code'], status=14)

    def fetch(self, task, callback=None):
        if self.async_mode:
            return self.async_fetch(task, callback)
        else:
            return self.async_fetch(task, callback).result()

    @gen.coroutine
    def async_fetch(self, task, callback=None, retry_times=None):
        '''Do one fetch'''
        url = task.get('url', 'data:,')
        if callback is None:
            callback = self.send_result

        type = 'None'
        start_time = time.time()
        try:
            if url.startswith('data:'):
                type = 'data'
                result = yield gen.maybe_future(self.data_fetch(url, task))
            elif task.get('fetch', {}).get('fetch_type') in ('js', 'phantomjs'):
                type = 'phantomjs'
                result = yield self.phantomjs_fetch(url, task)
            elif task.get('fetch', {}).get('fetch_type') in ('splash', ):
                type = 'splash'
                result = yield self.splash_fetch(url, task)
            elif task.get('fetch', {}).get('fetch_type') in ('webdriver', ):
                type = 'webdriver'
                logger.debug("fetcher webdriver task is %s:%s, queue name is %s"%(task['project'], task['taskid'], task.get('schedule', {}).get('queue_name')))
                result = yield self.webdriver_fetch(url, task)
            elif task.get('fetch', {}).get('fetch_type') in ('chrome', 'ch'):
                type = 'chrome'
                result = yield self.chrome_fetch(url, task)
            elif task.get('fetch', {}).get('fetch_type') in ('selenium_phantomjs_fetch', 'sp'):
                type = 'selenium_phantomjs'
                result = yield self.selenium_phantomjs_fetch(url, task)
            else:
                type = 'http'
                result = yield self.http_fetch(url, task)
        except Exception as e:
            logger.exception(e)
            result = self.handle_error(type, url, task, start_time, e)
            if self.processdb is not None:
                self.processdb.update_status(project=task['project'], taskid=task['taskid'], fetcher_response_code=result['status_code'], status=13)
        if result['status_code'] == 521:
            result['status_code'] = 200
            if 'error' in result:
                del result['error']
        if result['status_code'] == 201:
            if 'error' in result:
                del result['error']
        if task.get('fetch', {}).get('sequence'):
            result['sequence'] = int(task.get('fetch', {}).get('sequence')) - 1
        if task.get('fetch', {}).get('page_num'):
            result['page_num'] = int(task.get('fetch', {}).get('page_num')) - 1
        result['group'] = task.get('group')
        if retry_times is not None:
            return result
        if (task.get('fetch', {}).get('proxy') or task.get('fetch', {}).get('proxy_host')) and result.get('status_code') != 200 and retry_times is None:
            proxy = task.get('fetch', {}).get('proxy')[7:-1] if task.get('fetch', {}).get('proxy') else '%s:%s'%(task.get('fetch', {}).get('proxy_host'), task.get('fetch', {}).get('proxy_port'))
            if result.get('status_code') == 599:
                self.proxypool.complain(proxy)
                for index in range(self.proxy_retry_times):
                    task['fetch'].update(self.pack_proxy_parameters(self.proxypooldb.getPos(proxy)))
                    proxy = task.get('fetch', {}).get('proxy')[7:-1] if task.get('fetch', {}).get('proxy') else '%s:%s'%(task.get('fetch', {}).get('proxy_host'), task.get('fetch', {}).get('proxy_port'))
                    result = yield self.async_fetch(task, callback, index)
                    if result.get('status_code') == 200:
                        break
                    elif result.get('status_code') == 599:
                        self.proxypool.complain(proxy)
        if task.get('fetch', {}).get('headers') and isinstance(task.get('fetch', {})['headers'], tornado.httputil.HTTPHeaders):
            task.get('fetch', {})['headers'] = task.get('fetch', {})['headers']._dict

        callback(type, task, result)
        self.on_result(type, task, result)
        if self.fetcherrorprojectdb:
            if result.get('status_code') and result['status_code'] != 200 and result['status_code'] != 304:
                self.fetcherrorprojectdb.set_error(task['project'], task['taskid'])
            elif (result['status_code'] == 200 or result['status_code'] == 304) and re.search('^[0-9a-zA-Z]+$', task['taskid']):
                self.fetcherrorprojectdb.drop(task['project'])
        raise gen.Return(result)

    def sync_fetch(self, task):
        '''Synchronization fetch, usually used in xmlrpc thread'''
        if not self._running:
            return self.ioloop.run_sync(functools.partial(self.async_fetch, task, lambda t, _, r: True))

        wait_result = threading.Condition()
        _result = {}

        def callback(type, task, result):
            wait_result.acquire()
            _result['type'] = type
            _result['task'] = task
            _result['result'] = result
            wait_result.notify()
            wait_result.release()

        wait_result.acquire()
        self.ioloop.add_callback(self.fetch, task, callback)
        while 'result' not in _result:
            wait_result.wait()
        wait_result.release()
        return _result['result']
       

    def data_fetch(self, url, task):
        '''A fake fetcher for dataurl'''
        self.on_fetch('data', task)
        result = {}
        result['orig_url'] = url
        result['content'] = dataurl.decode(url)
        result['headers'] = {}
        result['status_code'] = 200
        result['url'] = url
        result['cookies'] = {}
        result['time'] = 0
        result['save'] = task.get('fetch', {}).get('save')
        if len(result['content']) < 70:
            logger.info("[200] %s:%s %s 0s", task.get('project'), task.get('taskid'), url)
        else:
            logger.info(
                "[200] %s:%s data:,%s...[content:%d] 0s",
                task.get('project'), task.get('taskid'),
                result['content'][:70],
                len(result['content'])
            )

        return result

    def handle_error(self, type, url, task, start_time, error):
        result = {
            'status_code': getattr(error, 'code', 599),
            'error': utils.text(error),
            'traceback': traceback.format_exc() if sys.exc_info()[0] else None,
            'content': "",
            'time': time.time() - start_time,
            'orig_url': url,
            'url': url,
            "save": task.get('fetch', {}).get('save')
        }
        logger.error("[%d] %s:%s %s, %r %.2fs",
                     result['status_code'], task.get('project'), task.get('taskid'),
                     url, error, result['time'])
        return result

    allowed_options = ['method', 'data', 'connect_timeout', 'timeout', 'cookies', 'use_gzip', 'validate_cert']

    def pack_proxy_parameters(self, pos):
        fetch = dict()
        proxy_string = self.proxypool.getProxy(pos)
        if proxy_string:
            if '://' not in proxy_string:
                proxy_string = 'http://' + proxy_string
            proxy_splited = urlsplit(proxy_string)
            fetch['proxy_host'] = proxy_splited.hostname
            if proxy_splited.username:
                fetch['proxy_username'] = proxy_splited.username
            if proxy_splited.password:
                fetch['proxy_password'] = proxy_splited.password
            if six.PY2:
                for key in ('proxy_host', 'proxy_username', 'proxy_password'):
                    if key in fetch:
                        fetch[key] = fetch[key].encode('utf8')
            fetch['proxy_port'] = proxy_splited.port or 8080
            fetch['proxy'] = 'http://%s:%s/'%(fetch['proxy_host'], fetch['proxy_port'])
        return fetch

    def pack_tornado_request_parameters(self, url, task):
        fetch = copy.deepcopy(self.default_options)
        fetch['url'] = url
        fetch['headers'] = tornado.httputil.HTTPHeaders(fetch['headers'])
        fetch['headers']['User-Agent'] = self.user_agent
        task_fetch = task.get('fetch', {})
        for each in self.allowed_options:
            if each in task_fetch:
                fetch[each] = task_fetch[each]
        fetch['headers'].update(task_fetch.get('headers', {}))

        if task.get('track'):
            track_headers = tornado.httputil.HTTPHeaders(
                task.get('track', {}).get('fetch', {}).get('headers') or {})
            track_ok = task.get('track', {}).get('process', {}).get('ok', False)
        else:
            track_headers = {}
            track_ok = False
        # proxy
        proxy_string = None
        if isinstance(task_fetch.get('proxy'), six.string_types):
            proxy_string = task_fetch['proxy']
        elif self.proxy and task_fetch.get('proxy', True):
            proxy_string = self.proxy

        if task.get('use_proxy') is not None and str(task.get('use_proxy')).lower() == 'true' and self.proxypool is not None:
            # TODO 遍历获取
            proxy_string = self.proxypool.getProxy() if not proxy_string else proxy_string

        if proxy_string:
            if '://' not in proxy_string:
                proxy_string = 'http://' + proxy_string
            proxy_splited = urlsplit(proxy_string)
            fetch['proxy_host'] = proxy_splited.hostname
            if proxy_splited.username:
                fetch['proxy_username'] = proxy_splited.username
            if proxy_splited.password:
                fetch['proxy_password'] = proxy_splited.password
            if six.PY2:
                for key in ('proxy_host', 'proxy_username', 'proxy_password'):
                    if key in fetch:
                        fetch[key] = fetch[key].encode('utf8')
            fetch['proxy_port'] = proxy_splited.port or 8080

        # etag
        if task_fetch.get('etag', True):
            _t = None
            if isinstance(task_fetch.get('etag'), six.string_types):
                _t = task_fetch.get('etag')
            elif track_ok:
                _t = track_headers.get('etag')
            if _t and 'If-None-Match' not in fetch['headers']:
                fetch['headers']['If-None-Match'] = _t
        # last modifed
        if task_fetch.get('last_modified', task_fetch.get('last_modifed', True)):
            last_modified = task_fetch.get('last_modified', task_fetch.get('last_modifed', True))
            _t = None
            if isinstance(last_modified, six.string_types):
                _t = last_modified
            elif track_ok:
                _t = track_headers.get('last-modified')
            if _t and 'If-Modified-Since' not in fetch['headers']:
                fetch['headers']['If-Modified-Since'] = _t
        # timeout
        if 'timeout' in fetch:
            fetch['request_timeout'] = fetch['timeout']
            del fetch['timeout']
        # data rename to body
        if 'data' in fetch:
            fetch['body'] = fetch['data']
            del fetch['data']

        return fetch

    @gen.coroutine
    def can_fetch(self, user_agent, url):
        parsed = urlsplit(url)
        domain = parsed.netloc
        if domain in self.robots_txt_cache:
            robot_txt = self.robots_txt_cache[domain]
            if time.time() - robot_txt.mtime() > self.robot_txt_age:
                robot_txt = None
        else:
            robot_txt = None

        if robot_txt is None:
            robot_txt = RobotFileParser()
            try:
                response = yield gen.maybe_future(self.http_client.fetch(
                    urljoin(url, '/robots.txt'), connect_timeout=70, request_timeout=60))
                content = response.body
            except tornado.httpclient.HTTPError as e:
                logger.error('load robots.txt from %s error: %r', domain, e)
                content = ''

            try:
                content = content.decode('utf8', 'ignore')
            except UnicodeDecodeError:
                content = ''

            robot_txt.parse(content.splitlines())
            self.robots_txt_cache[domain] = robot_txt

        raise gen.Return(robot_txt.can_fetch(user_agent, url))

    def clear_robot_txt_cache(self):
        now = time.time()
        for domain, robot_txt in self.robots_txt_cache.items():
            if now - robot_txt.mtime() > self.robot_txt_age:
                del self.robots_txt_cache[domain]
    

    @gen.coroutine
    def selenium_phantomjs_fetch(self, url, task):
        '''chrome fetcher'''
        start_time = time.time()
        result = {}
        # result['orig_url'] = url
        task_fetch = task.get('fetch', {})
        try:           
            dcap = dict(DesiredCapabilities.PHANTOMJS)
            dcap["phantomjs.page.settings.userAgent"] = ("Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; InfoPath.3)")
            obj = webdriver.PhantomJS(executable_path='/home/paas/local/software/phantomjs-2.1.1-linux-x86_64/bin/phantomjs', desired_capabilities=dcap) #加载网址
            # obj = webdriver.PhantomJS(executable_path='D:/phantomjs/phantomjs-2.1.1-windows/bin/phantomjs', desired_capabilities=dcap) #加载网址
            obj.set_page_load_timeout(100)
            obj.maximize_window()
            obj.get(url)
            # element = WebDriverWait(obj, 20, 1).until(
            #     EC.(obj.page_source.startswith('<!DOCTYPE'))
            #     )
            time.sleep(3)
            num = 97
            while (obj.page_source).find('下一页') == -1:
                print("----------没爬到列表页----------" + str(num))
                time.sleep(1)
                num = num -1
                if num < 0:
                    break
            print("----------爬取到列表页----------")
            result = {'orig_url':url, 'content':obj.page_source, 'headers':{}, 'status_code':200, 'url':obj.current_url or url, 'cookies':{}, 'time':time.time() - start_time, 'save':task_fetch.get('save')}
            obj.quit()
        except Exception as e:
            result = {}
            result = {'orig_url':url, 'status_code':500, 'url':url, 'cookies':{}, 'time':time.time() - start_time, 'save':task_fetch.get('save')}
        raise gen.Return(result)


    @gen.coroutine
    def chrome_fetch(self, url, task):
        '''chrome fetcher'''
        start_time = time.time()
        result = {}
        # result['orig_url'] = url
        task_fetch = task.get('fetch', {})
        try:           
            chrome_options = Options()
            chrome_options.add_argument('window-size=1920x3000')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--hide-scrollbars')
            chrome_options.add_argument('blink-settings=imagesEnabled=false')
            chrome_options.add_argument('--headless')
            chrome_options.binary_location = r"C:\\Users\\mengqy-a\\AppData\\Local\\Google\\Chrome\\Application\\chrome.exe"
            driver = webdriver.Chrome(chrome_options=chrome_options)
            driver.get(url)
            # result['content'] = driver.page_source
            # result['headers'] = {}
            # result['status_code'] = 200
            # result['url'] = driver.current_url or url
            # result['cookies'] = {}
            result = {'orig_url':url, 'content':driver.page_source, 'headers':{}, 'status_code':200, 'url':driver.current_url or url, 'cookies':{}, 'time':time.time() - start_time, 'save':task_fetch.get('save')}
            driver.close()
        except Exception as e:
            result = {}
            # result['orig_url'] = url
            # result['status_code'] = 500
            # result['url'] = url
            # result['cookies'] = {}
            result = {'orig_url':url, 'status_code':500, 'url':url, 'cookies':{}, 'time':time.time() - start_time, 'save':task_fetch.get('save')}
        raise gen.Return(result)



    @gen.coroutine
    def http_fetch(self, url, task):
        '''HTTP fetcher'''
        start_time = time.time()
        self.on_fetch('http', task)
        handle_error = lambda x: self.handle_error('http', url, task, start_time, x)
        # setup request parameters
        fetch = self.pack_tornado_request_parameters(url, task)
        task['fetch'].update(fetch)
        task_fetch = task.get('fetch', {})

        session = cookies.RequestsCookieJar()
        # fix for tornado request obj
        if 'Cookie' in fetch['headers']:
            c = http_cookies.SimpleCookie()
            try:
                c.load(fetch['headers']['Cookie'])
            except AttributeError:
                c.load(utils.utf8(fetch['headers']['Cookie']))
            for key in c:
                session.set(key, c[key])
            del fetch['headers']['Cookie']
        if 'cookies' in fetch:
            session.update(fetch['cookies'])
            del fetch['cookies']

        max_redirects = task_fetch.get('max_redirects', 5)
        # we will handle redirects by hand to capture cookies
        fetch['follow_redirects'] = False

        # making requests
        while True:
            # robots.txt
            if task_fetch.get('robots_txt', False):
                can_fetch = yield self.can_fetch(fetch['headers']['User-Agent'], fetch['url'])
                if not can_fetch:
                    error = tornado.httpclient.HTTPError(403, 'Disallowed by robots.txt')
                    raise gen.Return(handle_error(error))

            try:
                request = tornado.httpclient.HTTPRequest(**fetch)
                # if cookie already in header, get_cookie_header wouldn't work
                old_cookie_header = request.headers.get('Cookie')
                if old_cookie_header:
                    del request.headers['Cookie']
                cookie_header = cookies.get_cookie_header(session, request)
                if cookie_header:
                    request.headers['Cookie'] = cookie_header
                elif old_cookie_header:
                    request.headers['Cookie'] = old_cookie_header
            except Exception as e:
                logger.exception(fetch)
                raise gen.Return(handle_error(e))

            if not (True if str(task.get('fetch', {}).get('skip_fetcher')).lower() == 'true' else False):
                try:
                    response = yield gen.maybe_future(self.http_client.fetch(request))
                except tornado.httpclient.HTTPError as e:
                    if e.response:
                        response = e.response
                    else:
                        raise gen.Return(handle_error(e))

                extract_cookies_to_jar(session, response.request, response.headers)
                if (response.code in (301, 302, 303, 307)
                        and response.headers.get('Location')
                        and task_fetch.get('allow_redirects', True)):
                    if max_redirects <= 0:
                        error = tornado.httpclient.HTTPError(
                            599, 'Maximum (%d) redirects followed' % task_fetch.get('max_redirects', 5),
                            response)
                        raise gen.Return(handle_error(error))
                    if response.code in (302, 303):
                        fetch['method'] = 'GET'
                        if 'body' in fetch:
                            del fetch['body']
                    fetch['url'] = quote_chinese(urljoin(fetch['url'], response.headers['Location']))
                    fetch['request_timeout'] -= time.time() - start_time
                    if fetch['request_timeout'] < 0:
                        fetch['request_timeout'] = 0.1
                    max_redirects -= 1
                    continue

            result = {}
            result['orig_url'] = url
            if not (True if str(task.get('fetch', {}).get('skip_fetcher')).lower() == 'true' else False):
                result['content'] = response.body or ''
                result['headers'] = dict(response.headers)
                result['status_code'] = response.code
                result['url'] = response.effective_url or url
            else:
                result['status_code'] = 200
                result['url'] = url
            result['time'] = time.time() - start_time
            result['cookies'] = session.get_dict()
            result['save'] = task_fetch.get('save')

            if not (True if str(task.get('fetch', {}).get('skip_fetcher')).lower() == 'true' else False):
                if response.error:
                    result['error'] = utils.text(response.error)
                if 200 <= response.code < 300:
                    logger.info("[%d] %s:%s %s %.2fs", response.code,
                                task.get('project'), task.get('taskid'),
                                url, result['time'])
                else:
                    logger.warning("[%d] %s:%s %s %.2fs", response.code,
                                   task.get('project'), task.get('taskid'),
                                   url, result['time'])
            result['configure'] = self.configure
            result['project_name'] = task.get('project')
            raise gen.Return(result)

    @gen.coroutine
    def webdriver_fetch(self, url, task):
        start_time = time.time()
        self.on_fetch('phantomjs', task)
        handle_error = lambda x: self.handle_error('http', url, task, start_time, x)
        try:
            if url is not None and (not task.get('fetch', {}).get('css_selector') and not task.get('fetch', {}).get('xpath_selector')):
                driver = self.drivers.get_driver(task.get('project'), True,  task.get('fetch', {}).get('load_img'))
                try:
                    driver.get(url)
                except WebDriverException:
                    self.drivers.delete_driver(task.get('project'))
                    driver = self.drivers.get_driver(task.get('project'), True, task.get('fetch', {}).get('load_img'))
                    driver.get(url)
                if task.get('fetch', {}).get('wait_for_xpath'):
                    WebDriverWait(driver, 1, 0.5).until(EC.element_to_be_clickable((By.XPATH, task.get('fetch', {}).get('wait_for_xpath'))))
                origin_url = driver.current_url
                content = driver.page_source if task.get('fetch', {}).get('encoder') is False else bytes(driver.page_source, encoding="utf8")
                url = origin_url
            elif task.get('fetch', {}).get('css_selector') or task.get('fetch', {}).get('xpath_selector'):
                # assert self.drivers.get_driver(task.get('project')), 'no webdriver'
                if not self.drivers.get_driver(task.get('project')):
                    logging.info('webdriver has gone, i can not continue to work')
                    error = tornado.httpclient.HTTPError(201, 'webdriver has gone')
                    raise gen.Return(handle_error(error))
                driver = self.drivers.get_driver(task.get('project'))
                origin_url = driver.current_url
                source_handle = driver.current_window_handle
                source_html = driver.find_element_by_xpath(task.get('fetch', {}).get('wait_for_xpath')).text if task.get('fetch', {}).get('wait_for_xpath') else ''
                self.webdriver_oper(driver, task.get('fetch', {}).get('css_selector'), task.get('fetch', {}).get('xpath_selector'))
                if task.get('fetch', {}).get('wait_for_xpath'):
                    WebDriverWait(driver, 1, 0.5).until_not(EC.text_to_be_present_in_element((By.XPATH, task.get('fetch', {}).get('wait_for_xpath')), source_html))
                # if task.get('fetch', {}).get('css_selector'):
#                     WebDriverWait(driver,10, 0.5).until(EC.element_to_be_clickable((By.CSS_SELECTOR, task.get('fetch', {}).get('css_selector'))))
#                     element = driver.find_element_by_css_selector(task.get('fetch', {}).get('css_selector'))
#                     driver.execute_script("arguments[0].scrollIntoView()", element)
#                     ActionChains(driver).move_to_element(element).click().perform()
#                     driver.switch_to_window(driver.window_handles[-1])
#                 if task.get('fetch', {}).get('xpath_selector'):
#                     WebDriverWait(driver,10, 0.5).until(EC.element_to_be_clickable((By.XPATH, task.get('fetch', {}).get('xpath_selector'))))
#                     element = driver.find_element_by_xpath(task.get('fetch', {}).get('xpath_selector'))
#                     driver.execute_script("arguments[0].scrollIntoView()", element)
#                     ActionChains(driver).move_to_element(element).click().perform()
#                     driver.switch_to_window(driver.window_handles[-1])
                window_handle = driver.current_window_handle
                content = driver.page_source if task.get('fetch', {}).get('encoder') is False else bytes(driver.page_source, encoding="utf8")
                url = driver.current_url
                if task.get('fetch', {}).get('is_final'):
                    if window_handle == source_handle:
                        driver.back()
                    else:
                        driver.close()
                        driver.switch_to_window(driver.window_handles[-1])
            result = {
                    "orig_url": origin_url,
                    "content": content,
                    "status_code": 200,
                    "headers": {},
                    "url": url,
                    "cookies": {},
                    "time": time.time() - start_time,
                    "js_script_result": None,
                    "save": task.get('fetch', {}).get('save')
            }
        except TimeoutException as e:
            result = {
                    "orig_url": url,
                    "content": "load page is timeout",
                    "headers": {},
                    "status_code": 504,
                    "url": url,
                    "time": time.time() - start_time,
                    "cookies": {},
                    "save": task.get('fetch', {}).get('save')
            }
            logger.warning("[504] webdriver timeout %s:%s %s 0s", task.get('project'), task.get('taskid'), url)
            logger.warning("webdriver timeout error is %s"%(e.msg))
        except WebDriverException as e:
            result = {
                "orig_url": url,
                "content": "webdriver not found",
                "headers": {},
                "status_code": 500,
                "url": url,
                "time": time.time() - start_time,
                "cookies": {},
                "save": task.get('fetch', {}).get('save')
            }
            logger.error("[500] webdriver not found %s:%s %s 0s", task.get('project'), task.get('taskid'), url)
            logger.error("webdriver error:%s"%(e.msg))
            self.drivers.delete_driver(task.get('project'))
        result['configure'] = self.configure
        result['project_name'] = task.get('project')
        raise gen.Return(result)

    @gen.coroutine
    def phantomjs_fetch(self, url, task):
        '''Fetch with phantomjs proxy'''
        start_time = time.time()
        self.on_fetch('phantomjs', task)
        handle_error = lambda x: self.handle_error('phantomjs', url, task, start_time, x)
        # check phantomjs proxy is enabled
        if not self.phantomjs_proxy:
            result = {
                "orig_url": url,
                "content": "phantomjs is not enabled.",
                "headers": {},
                "status_code": 501,
                "url": url,
                "time": time.time() - start_time,
                "cookies": {},
                "save": task.get('fetch', {}).get('save'),
                "configure": self.configure
            }
            logger.warning("[501] %s:%s %s 0s", task.get('project'), task.get('taskid'), url)
            result['project_name'] = task.get('project')
            raise gen.Return(result)

        # setup request parameters
        fetch = self.pack_tornado_request_parameters(url, task)
        task_fetch = task.get('fetch', {})
        for each in task_fetch:
            if each not in fetch:
                fetch[each] = task_fetch[each]

        # robots.txt
        if task_fetch.get('robots_txt', False):
            user_agent = fetch['headers']['User-Agent']
            can_fetch = yield self.can_fetch(user_agent, url)
            if not can_fetch:
                error = tornado.httpclient.HTTPError(403, 'Disallowed by robots.txt')
                raise gen.Return(handle_error(error))

        request_conf = {
            'follow_redirects': False
        }
        request_conf['connect_timeout'] = fetch.get('connect_timeout', 70)
        request_conf['request_timeout'] = fetch.get('request_timeout', 60) + 1

        session = cookies.RequestsCookieJar()
        if 'Cookie' in fetch['headers']:
            c = http_cookies.SimpleCookie()
            try:
                c.load(fetch['headers']['Cookie'])
            except AttributeError:
                c.load(utils.utf8(fetch['headers']['Cookie']))
            for key in c:
                session.set(key, c[key])
            del fetch['headers']['Cookie']
        if 'cookies' in fetch:
            session.update(fetch['cookies'])
            del fetch['cookies']

        request = tornado.httpclient.HTTPRequest(url=fetch['url'])
        cookie_header = cookies.get_cookie_header(session, request)
        if cookie_header:
            fetch['headers']['Cookie'] = cookie_header

        # making requests
        fetch['headers'] = dict(fetch['headers'])
        try:
            request = tornado.httpclient.HTTPRequest(
                url=self.phantomjs_proxy, method="POST",
                body=json.dumps(fetch), **request_conf)
        except Exception as e:
            raise gen.Return(handle_error(e))

        try:
            response = yield gen.maybe_future(self.http_client.fetch(request))
        except tornado.httpclient.HTTPError as e:
            if e.response:
                response = e.response
            else:
                raise gen.Return(handle_error(e))

        if not response.body:
            raise gen.Return(handle_error(Exception('no response from phantomjs: %r' % response)))

        result = {}
        try:
            result = json.loads(utils.text(response.body))
            assert 'status_code' in result, result
        except Exception as e:
            if response.error:
                result['error'] = utils.text(response.error)
            raise gen.Return(handle_error(e))

        result['body'] = response.body

        # error错误为“Timeout before first response”时且页面获取正常时，放过去
        if result.get('error', None) == 'Timeout before first response.' and result.get('status_code', None) == 599:
            del result['error']
            result['status_code'] = 200

        if result.get('status_code', 200):
            logger.info("[%d] %s:%s %s %.2fs", result['status_code'],
                        task.get('project'), task.get('taskid'), url, result['time'])
        else:
            logger.error("[%d] %s:%s %s, %r %.2fs", result['status_code'],
                         task.get('project'), task.get('taskid'),
                         url, result['content'], result['time'])
        result['configure'] = self.configure
        result['project_name'] = task.get('project')
        raise gen.Return(result)

    @gen.coroutine
    def splash_fetch(self, url, task):
        '''Fetch with splash'''
        start_time = time.time()
        self.on_fetch('splash', task)
        handle_error = lambda x: self.handle_error('splash', url, task, start_time, x)

        # check phantomjs proxy is enabled
        if not self.splash_endpoint:
            result = {
                "orig_url": url,
                "content": "splash is not enabled.",
                "headers": {},
                "status_code": 501,
                "url": url,
                "time": time.time() - start_time,
                "cookies": {},
                "save": task.get('fetch', {}).get('save'),
                "configure": self.configure
            }
            logger.warning("[501] %s:%s %s 0s", task.get('project'), task.get('taskid'), url)
            result['configure'] = self.configure
            result['project_name'] = task.get('project')
            raise gen.Return(result)

        # setup request parameters
        fetch = self.pack_tornado_request_parameters(url, task)
        task_fetch = task.get('fetch', {})
        for each in task_fetch:
            if each not in fetch:
                fetch[each] = task_fetch[each]

        # robots.txt
        if task_fetch.get('robots_txt', False):
            user_agent = fetch['headers']['User-Agent']
            can_fetch = yield self.can_fetch(user_agent, url)
            if not can_fetch:
                error = tornado.httpclient.HTTPError(403, 'Disallowed by robots.txt')
                raise gen.Return(handle_error(error))

        request_conf = {
            'follow_redirects': False,
            'headers': {
                'Content-Type': 'application/json',
            }
        }
        request_conf['connect_timeout'] = fetch.get('connect_timeout', 70)
        request_conf['request_timeout'] = fetch.get('request_timeout', 60) + 1

        session = cookies.RequestsCookieJar()
        if 'Cookie' in fetch['headers']:
            c = http_cookies.SimpleCookie()
            try:
                c.load(fetch['headers']['Cookie'])
            except AttributeError:
                c.load(utils.utf8(fetch['headers']['Cookie']))
            for key in c:
                session.set(key, c[key])
            del fetch['headers']['Cookie']
        if 'cookies' in fetch:
            session.update(fetch['cookies'])
            del fetch['cookies']

        request = tornado.httpclient.HTTPRequest(url=fetch['url'])
        cookie_header = cookies.get_cookie_header(session, request)
        if cookie_header:
            fetch['headers']['Cookie'] = cookie_header

        # making requests
        fetch['lua_source'] = self.splash_lua_source
        fetch['headers'] = dict(fetch['headers'])
        try:
            request = tornado.httpclient.HTTPRequest(
                url=self.splash_endpoint, method="POST",
                body=json.dumps(fetch), **request_conf)
        except Exception as e:
            raise gen.Return(handle_error(e))

        try:
            response = yield gen.maybe_future(self.http_client.fetch(request))
        except tornado.httpclient.HTTPError as e:
            if e.response:
                response = e.response
            else:
                raise gen.Return(handle_error(e))

        if not response.body:
            raise gen.Return(handle_error(Exception('no response from phantomjs')))

        result = {}
        try:
            result = json.loads(utils.text(response.body))
            assert 'status_code' in result, result
        except ValueError as e:
            logger.error("result is not json: %r", response.body[:500])
            raise gen.Return(handle_error(e))
        except Exception as e:
            if response.error:
                result['error'] = utils.text(response.error)
            raise gen.Return(handle_error(e))

        if result.get('status_code', 200):
            logger.info("[%d] %s:%s %s %.2fs", result['status_code'],
                        task.get('project'), task.get('taskid'), url, result['time'])
        else:
            logger.error("[%d] %s:%s %s, %r %.2fs", result['status_code'],
                         task.get('project'), task.get('taskid'),
                         url, result['content'], result['time'])
        result['configure'] = self.configure
        result['project_name'] = task.get('project')
        raise gen.Return(result)

    def run(self):
        '''Run loop'''
        logger.info("fetcher starting...")

        def queue_loop():
            if not self.outqueue or not self.inqueue:
                return
            while not self._quit:
                try:
                    if self.outqueue.full():
                        break
                    if self.http_client.free_size() <= 0:
                        break
                    task = self.inqueue.get_nowait()
                    logger.debug('fetch get task is %s'%(task))
                    #过载保护直接忽略此任务，并记录过载保护状态
                    if self.fetcherrorprojectdb:
                        if self.fetcherrorprojectdb.is_fetch_error(task.get('project')):
                            logger.info('%s is overload, fetcher task will be not execute'%(task['project']))
                            self.processdb.update_status(project=task['project'], taskid=task['taskid'], status=15)
                            continue
                    # FIXME: decode unicode_obj should used after data selete from
                    # database, it's used here for performance
                    task = utils.decode_unicode_obj(task)
                    if self.processdb is not None:
                        self.processdb.update_status(project=task['project'], taskid=task['taskid'], status=11)
                    logger.debug('fetcher will be work from queue %s get one task %s'%(self.inqueue.name, task))
                    self.fetch(task)
                except queue.Empty:
                    break
                except KeyboardInterrupt:
                    break
                except Exception as e:
                    logger.exception(e)
                    break

        tornado.ioloop.PeriodicCallback(queue_loop, 100, io_loop=self.ioloop).start()
        tornado.ioloop.PeriodicCallback(self.clear_robot_txt_cache, 10000, io_loop=self.ioloop).start()
        self._running = True

        try:
            self.ioloop.start()
        except KeyboardInterrupt:
            pass

        logger.info("fetcher exiting...")

    def quit(self):
        '''Quit fetcher'''
        self._running = False
        self._quit = True
        self.ioloop.add_callback(self.ioloop.stop)
        if hasattr(self, 'xmlrpc_server'):
            self.xmlrpc_ioloop.add_callback(self.xmlrpc_server.stop)
            self.xmlrpc_ioloop.add_callback(self.xmlrpc_ioloop.stop)

    def size(self):
        return self.http_client.size()

    def xmlrpc_run(self, port=24444, bind='127.0.0.1', logRequests=False):
        '''Run xmlrpc server'''
        import umsgpack
        from pyspider.libs.wsgi_xmlrpc import WSGIXMLRPCApplication
        try:
            from xmlrpc.client import Binary
        except ImportError:
            from xmlrpclib import Binary

        application = WSGIXMLRPCApplication()

        application.register_function(self.quit, '_quit')
        application.register_function(self.size)

        def sync_fetch(task):
            result = self.sync_fetch(task)
            result = Binary(umsgpack.packb(result))
            return result
        application.register_function(sync_fetch, 'fetch')

        def dump_counter(_time, _type):
            return self._cnt[_time].to_dict(_type)
        application.register_function(dump_counter, 'counter')

        import tornado.wsgi
        import tornado.ioloop
        import tornado.httpserver

        container = tornado.wsgi.WSGIContainer(application)
        self.xmlrpc_ioloop = tornado.ioloop.IOLoop()
        self.xmlrpc_server = tornado.httpserver.HTTPServer(container, io_loop=self.xmlrpc_ioloop)
        self.xmlrpc_server.listen(port=port, address=bind)
        logger.info('fetcher.xmlrpc listening on %s:%s', bind, port)
        self.xmlrpc_ioloop.start()

    def webdriver_oper(self, driver, css_path=None, xpath=None):
        if css_path:
            css_path_list = list()
            if isinstance(css_path, str):
                css_path_list.append(css_path)
            elif isinstance(css_path, list):
                css_path_list = css_path
            for css_path_item in css_path_list:
                WebDriverWait(driver,1, 0.5).until(EC.element_to_be_clickable((By.CSS_SELECTOR, css_path_item)))
                element = driver.find_element_by_css_selector(css_path_item)
                driver.execute_script("arguments[0].scrollIntoView()", element)
                ActionChains(driver).move_to_element(element).click().perform()
            driver.switch_to_window(driver.window_handles[-1])
        if xpath:
            xpath_list = list()
            if isinstance(xpath, str):
                xpath_list.append(xpath)
            elif isinstance(xpath, list):
                xpath_list = xpath
            for xpath_item in xpath_list:
                WebDriverWait(driver,1, 0.5).until(EC.element_to_be_clickable((By.XPATH, xpath_item)))
                element = driver.find_element_by_xpath(xpath_item)
                driver.execute_script("arguments[0].scrollIntoView()", element)
                ActionChains(driver).move_to_element(element).click().perform()
            driver.switch_to_window(driver.window_handles[-1])
    def on_fetch(self, type, task):
        '''Called before task fetch'''
        pass

    def on_result(self, type, task, result):
        '''Called after task fetched'''
        status_code = result.get('status_code', 599)
        if status_code != 599:
            status_code = (int(status_code) / 100 * 100)
        self._cnt['5m'].event((task.get('project'), status_code), +1)
        self._cnt['1h'].event((task.get('project'), status_code), +1)

        if type in ('http', 'phantomjs') and result.get('time'):
            content_len = len(result.get('content', ''))
            self._cnt['5m'].event((task.get('project'), 'speed'),
                                  float(content_len) / result.get('time'))
            self._cnt['1h'].event((task.get('project'), 'speed'),
                                  float(content_len) / result.get('time'))
            self._cnt['5m'].event((task.get('project'), 'time'), result.get('time'))
            self._cnt['1h'].event((task.get('project'), 'time'), result.get('time'))
