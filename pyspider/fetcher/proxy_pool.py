import time,hashlib
import logging
import urllib3

logger = logging.getLogger('proxypool')

class ProxyPool(object):
    def __init__(self, proxypooldb, lifetime, proxyname, proxyparam=None):
        self.proxypooldb = proxypooldb
        self.lifetime = lifetime
        self.proxyname = proxyname
        self.proxyparam = proxyparam

    def getProxy(self, pos=None):
        poolsize = self.proxypooldb.getPoolSize()
        nextPos = None
        if poolsize == 0:
            nextPos = self.addProxy()
        indexes = self.proxypooldb.getIndexes()
        if pos is not None:
            for key in indexes:
                index = self.proxypooldb.getIndex(key)
                if index > pos:
                    nextPos = index
                    break
            if nextPos is None:
                nextPos = self.addProxy()
        else:
            nextPos = self.proxypooldb.getIndex(indexes[0])

        if nextPos is not None:
            return self.proxypooldb.getProxy(nextPos)
        else:
            return None

    def getNewProxy(self):
        if self.proxyname == 'jiguang':
            url = 'http://d.jghttp.golangapi.com/getip?num=1&type=1&pro=&city=0&yys=0&port=1&time=3&ts=0&ys=0&cs=0&lb=1&sb=0&pb=4&mr=1&regions='
            r = urllib3.PoolManager().request('GET', url)
            if r.status == 200:
                return str(r.data.decode()).replace('\r\n', '')
            else:
                return None
        else:
            return None

    def addProxy(self):
        maxposBeforeLock = self.proxypooldb.getMaxPos()
        res = self.proxypooldb.lockPool()
        maxposAfterLock = self.proxypooldb.getMaxPos()
        if res and maxposBeforeLock == maxposAfterLock:
            pos = self.proxypooldb.newPos()
            proxy = self.getNewProxy()
            if proxy is not None:
                self.proxypooldb.addProxy(pos, proxy, self.lifetime)
                self.proxypooldb.addIndex(pos, self.lifetime)
            self.proxypooldb.releasePool()
            if proxy is not None:
                return pos
            else:
                logger.error('cat not get proxy from supplier')
                return None
        else:
            indexes = self.proxypooldb.getIndexes()
            return self.proxypooldb.getIndex(indexes[len(indexes) - 1])

    def getProxyHeaders(self):
        return list()

# import os, sys
# sys.path.append(os.path.join(os.path.abspath(os.path.dirname(__file__)) , '../'))
# from database.redis.proxypooldb import Proxypooldb
#
# proxypooldb = Proxypooldb()
# proxypool = ProxyPool(proxypooldb, 7200, 'dynamic.xiongmaodaili.com:8088', 'jiguang')
# # print(proxypool.getNewProxy())
# print(proxypool.getProxy())