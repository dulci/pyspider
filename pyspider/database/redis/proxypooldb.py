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

    def __init__(self, host='127.0.0.1', port=6379, password=None, db=0):
        self.redis = redis.StrictRedis(host=host, port=port, password=password, db=db)
        try:
            self.redis.scan(count=1)
            self.scan_available = True
        except Exception as e:
            logging.debug("redis_scan disabled: %r", e)
            self.scan_available = False

    def newPos(self):
        return self.redis.incr(self.__PREFIX__ + self.__POS_KEY__)

    def getMaxPos(self):
        maxpos = self.redis.get(self.__PREFIX__ + self.__POS_KEY__)
        if maxpos is not None:
            return int(maxpos)
        else:
            return None

    def complain(self, pos, lifetime):
        reputation = self.redis.get(self.__PREFIX__ + self.__REPUTATION_KEY__ + str(pos))
        if reputation is None:
            self.redis.setex(self.__PREFIX__ + self.__REPUTATION_KEY__ + str(pos), lifetime, -1)
        else:
            self.redis.setex(self.__PREFIX__ + self.__REPUTATION_KEY__ + str(pos), lifetime, str(int(reputation) - 1))

    def encourage(self, pos, lifetime):
        reputation = self.redis.get(self.__PREFIX__ + self.__REPUTATION_KEY__ + str(pos))
        if reputation is None:
            self.redis.setex(self.__PREFIX__ + self.__REPUTATION_KEY__ + str(pos), lifetime, 1)
        else:
            self.redis.setex(self.__PREFIX__ + self.__REPUTATION_KEY__ + str(pos), lifetime, str(int(reputation) + 1))

    def getReputation(self, pos):
        reputation = self.redis.get(self.__PREFIX__ + self.__REPUTATION_KEY__ + str(pos))
        if reputation is None:
            return 0
        else:
            return int(reputation)

    def getProxy(self, pos):
        proxy = self.redis.get(self.__PREFIX__ + self.__PROXY_KEY__ + str(pos))
        if proxy is not None:
            return str(proxy, 'utf-8')
        else:
            return None

    def addProxy(self, pos, proxy, lifetime):
        self.redis.setex(self.__PREFIX__ + self.__PROXY_KEY__ + str(pos), lifetime, proxy)

    def addReputation(self, pos, lifetime):
        self.redis.setex(self.__PREFIX__ + self.__REPUTATION_KEY__ + str(pos), lifetime, str(0))

    def deleteProxy(self, pos):
        self.redis.delete(self.__PREFIX__ + self.__PROXY_KEY__ + str(pos))

    def deleteReputation(self, pos):
        self.redis.delete(self.__PREFIX__ + self.__REPUTATION_KEY__ + str(pos))

    def getPoolSize(self):
        return len(self.getIndexes())

    def getIndex(self, key):
        index = self.redis.get(key)
        if index is not None:
            return int(index)
        else:
            return None

    def addIndex(self, pos, lifetime):
        self.redis.setex(self.__PREFIX__ + self.__INDEX_KEY__ + str(pos), lifetime, str(pos))

    def deleteIndex(self, pos):
        self.redis.delete(self.__PREFIX__ + self.__INDEX_KEY__ + str(pos))
        self.deleteProxy(pos)
        self.deleteReputation(pos)

    def getIndexes(self):
        indexes = self.redis.keys(self.__PREFIX__ + self.__INDEX_KEY__ + "*")
        indexes.sort()
        return indexes

    def getReputations(self):
        indexes = self.redis.keys(self.__PREFIX__ + self.__REPUTATION_KEY__ + "*")
        indexes.sort()
        return indexes

    def getMaxReputation(self):
        indexes = self.getReputations()
        for reputation in sorted(indexes, key=lambda index: int(self.redis.get(index)), reverse=True):
            if self.redis.keys(self.__PREFIX__ + self.__INDEX_KEY__ + reputation.decode('utf-8').split('.')[-1]):
                return reputation

    def lockPool(self):
        self.lock = self.redis.lock(self.__PREFIX__ + self.__LOCK_KEY__, timeout=15, sleep=1, blocking_timeout=5, thread_local=False)
        return self.lock.acquire()

    def releasePool(self):
        if self.lock is not None:
            self.lock.release()
            self.lock = None

    def getPos(self, proxy):
        index = [x for x in self.redis.keys(self.__PREFIX__ + self.__PROXY_KEY__ + '*') if self.redis.get(x).decode('utf-8') == proxy]
        if index:
            return int(index[0].decode('utf-8').split('.')[-1])