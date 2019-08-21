#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# vim: set et sw=4 ts=4 sts=4 ff=unix fenc=utf8:
# Author: Binux<roy@binux.me>
#         http://binux.me
# Created on 2015-05-16 21:01:52

import six
import time
import json
import redis
import logging
import itertools

from pyspider.libs import utils
from pyspider.database.base.projectdb import ProjectDB as BaseProjectDB


class Proxypooldb(object):
    __PREFIX__ = "pyspider.proxypool."
    __POS_KEY__ = "position"
    __REPUTATION_KEY__ = "reputation."
    __PROXY_KEY__ = "proxy."
    __INDEX_KEY__ = "index."
    __LOCK_KEY__ = "lock"
    __HTTPS_PREFIX__ = "pyspider.proxypool.https."
    __HTTPS_POS_KEY__ = "position.https"
    __HTTPS_REPUTATION_KEY__ = "reputation.https."
    __HTTPS_PROXY_KEY__ = "proxy.https."
    __HTTPS_INDEX_KEY__ = "index.https."
    __HTTPS_LOCK_KEY__ = "lock.https"


    def __init__(self, host='127.0.0.1', port=6379, password=None, db=0):
        self.redis = redis.StrictRedis(host=host, port=port, password=password, db=db)
        self.lock = dict()
        try:
            self.redis.scan(count=1)
            self.scan_available = True
        except Exception as e:
            logging.debug("redis_scan disabled: %r", e)
            self.scan_available = False

    def newPos(self, protocol='http'):
        return self.redis.incr(self._prefix(protocol) + self._pos_key(protocol))

    def getMaxPos(self, protocol='http'):
        maxpos = self.redis.get(self._prefix(protocol) + self._pos_key(protocol))
        if maxpos is not None:
            return int(maxpos)
        else:
            return None

    def complain(self, pos, lifetime, protocol='http'):
        reputation = self.redis.get(self._prefix(protocol) + self._reputation_key(protocol) + str(pos))
        if reputation is None:
            self.redis.setex(self._prefix(protocol) + self._reputation_key(protocol) + str(pos), lifetime, -1)
        else:
            self.redis.setex(self._prefix(protocol) + self._reputation_key(protocol) + str(pos), lifetime, str(int(reputation) - 1))

    def encourage(self, pos, lifetime, protocol='http'):
        reputation = self.redis.get(self._prefix(protocol) + self._reputation_key(protocol) + str(pos))
        if reputation is None:
            self.redis.setex(self._prefix(protocol) + self._reputation_key(protocol) + str(pos), lifetime, 1)
        else:
            self.redis.setex(self._prefix(protocol) + self._reputation_key(protocol) + str(pos), lifetime, str(int(reputation) + 1))

    def getReputation(self, pos, protocol='http'):
        reputation = self.redis.get(self._prefix(protocol) + self._reputation_key(protocol) + str(pos))
        if reputation is None:
            return 0
        else:
            return int(reputation)

    def getProxy(self, pos, protocol='http'):
        proxy = self.redis.get(self._prefix(protocol) + self._proxy_key(protocol) + str(pos))
        if proxy is not None:
            return str(proxy, 'utf-8')
        else:
            return None

    def getProxies(self, protocol='http'):
        proxies = self.redis.keys(self._prefix(protocol) + self._proxy_key(protocol) + '*')
        if proxies is not None:
            return [self.redis.get(x).decode('utf-8') for x in proxies]
        else:
            return None

    def addProxy(self, pos, proxy, lifetime, protocol='http'):
        self.redis.setex(self._prefix(protocol) + self._proxy_key(protocol) + str(pos), lifetime, proxy)

    def addReputation(self, pos, lifetime, protocol='http'):
        self.redis.setex(self._prefix(protocol) + self._reputation_key(protocol) + str(pos), lifetime, str(0))

    def deleteProxy(self, pos, protocol='http'):
        self.redis.delete(self._prefix(protocol) + self._proxy_key(protocol) + str(pos))

    def deleteReputation(self, pos, protocol='http'):
        self.redis.delete(self._prefix(protocol) + self._reputation_key(protocol) + str(pos))

    def getPoolSize(self, protocol='http'):
        return len(self.getIndexes(protocol))

    def getIndex(self, key):
        index = self.redis.get(key)
        if index is not None:
            return int(index)
        else:
            return None

    def addIndex(self, pos, lifetime, protocol='http'):
        self.redis.setex(self._prefix(protocol) + self._index_key(protocol) + str(pos), lifetime, str(pos))

    def deleteIndex(self, pos, protocol='http'):
        self.redis.delete(self._prefix(protocol) + self._index_key(protocol) + str(pos))
        self.deleteProxy(pos, protocol)
        self.deleteReputation(pos, protocol)

    def deleteIndexByProxy(self, proxy, protocol):
        indexes = self.redis.keys(self._prefix(protocol) + self._proxy_key(protocol) + '*')
        for index in indexes:
            index_value = self.redis.get(index)
            if index_value and index_value.decode('utf-8') == proxy:
                pos = index_value.split('.')
                self.deleteIndex(pos, protocol)

    def getIndexes(self, protocol='http'):
        indexes = self.redis.keys(self._prefix(protocol) + self._index_key(protocol) + "*")
        indexes.sort()
        return indexes

    def getReputations(self, protocol='http'):
        indexes = self.redis.keys(self._prefix(protocol) + self._reputation_key(protocol) + "*")
        indexes.sort()
        return indexes

    def getMaxReputation(self, protocol='http'):
        indexes = self.getReputations(protocol)
        for reputation in sorted(indexes, key=lambda index: int(self.redis.get(index)), reverse=True):
            if self.redis.keys(self._prefix(protocol) + self._index_key(protocol) + reputation.decode('utf-8').split('.')[-1]):
                return reputation

    def lockPool(self, protocol='http'):
        self.lock[protocol] = self.redis.lock(self._prefix(protocol) + self._lock_key(protocol), timeout=15, sleep=1, blocking_timeout=5, thread_local=False)
        return self.lock[protocol].acquire()

    def releasePool(self, protocol='http'):
        if self.lock[protocol] is not None:
            self.lock[protocol].release()
            self.lock[protocol] = None

    def getPos(self, proxy, protocol='http'):
        index = [x for x in self.redis.keys(self._prefix(protocol) + self._proxy_key(protocol) + '*') if self.redis.get(x).decode('utf-8') == proxy]
        if index:
            return int(index[0].decode('utf-8').split('.')[-1])

    def _prefix(self, protocol='http'):
        if 'http' == protocol:
            return self.__PREFIX__
        elif 'https' == protocol:
            return self.__HTTPS_PREFIX__

    def _pos_key(self, protocol='http'):
        if 'http' == protocol:
            return self.__POS_KEY__
        elif 'https' == protocol:
            return self.__HTTPS_POS_KEY__

    def _reputation_key(self, protocol='http'):
        if 'http' == protocol:
            return self.__REPUTATION_KEY__
        elif 'https' == protocol:
            return self.__HTTPS_REPUTATION_KEY__

    def _proxy_key(self, protocol='http'):
        if 'http' == protocol:
            return self.__PROXY_KEY__
        elif 'https' == protocol:
            return self.__HTTPS_PROXY_KEY__

    def _index_key(self, protocol='http'):
        if 'http' == protocol:
            return self.__INDEX_KEY__
        elif 'https' == protocol:
            return self.__INDEX_KEY__

    def _lock_key(self, protocol='http'):
        if 'http' == protocol:
            return self.__LOCK_KEY__
        elif 'https' == protocol:
            return self.__HTTPS_LOCK_KEY__