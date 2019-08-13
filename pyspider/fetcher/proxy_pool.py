import time,hashlib
import logging
import urllib3
import re
import random
import time
logger = logging.getLogger('proxypool')

class ProxyPool(object):
    max_pool_size = 10
    fire_num = 2000
    def __init__(self, proxypooldb, lifetime, proxyname, proxyparam=None):
        self.proxypooldb = proxypooldb
        self.lifetime = lifetime
        self.proxyname = proxyname
        self.proxyparam = proxyparam

    def getProxy(self, pos=None, protocol='http'):
        poolsize = self.proxypooldb.getPoolSize(protocol)
        nextPos = None
        if poolsize < self.max_pool_size:
            while poolsize < self.max_pool_size:
                nextPos = self.addProxy(protocol)
                time.sleep(0.2)
                poolsize+=1
        indexes = self.proxypooldb.getIndexes(protocol)
        if pos is not None:
            for key in indexes:
                index = self.proxypooldb.getIndex(key)
                if index > pos:
                    nextPos = index
                    break
            if nextPos is None:
                if poolsize < self.max_pool_size + 1:
                    nextPos = self.addProxy(protocol)
                else:
                    maxReputationPos = self.getMaxReputationPos(protocol)
                    if maxReputationPos:
                        nextPos = maxReputationPos
                    else:
                        nextPos = self.proxypooldb.getIndex(indexes[0])
            if nextPos is not None:
                return self.proxypooldb.getProxy(nextPos, protocol)
        else:
            return self.getRandomProxy(protocol)
        return None

    def getMaxReputationPos(self, protocol='http'):
        maxReputation = self.proxypooldb.getMaxReputation(protocol)
        if maxReputation:
            return maxReputation.decode('utf-8').split('.')[-1]

    def getRandomProxy(self, protocol='http'):
        proxies = self.proxypooldb.getProxies(protocol)
        if proxies:
            index = random.randint(0,len(proxies)-1)
            return proxies[index]
        else:
            return None
    def getNewProxy(self, protocol='http'):
        if self.proxyname == 'jiguang':
            if 'http' == protocol:
                url = 'http://d.jghttp.golangapi.com/getip?num=1&type=1&pro=&city=0&yys=0&port=1&time=1&ts=0&ys=0&cs=0&lb=1&sb=0&pb=4&mr=1&regions='
            elif 'https' == protocol:
                url = 'http://d.jghttp.golangapi.com/getip?num=1&type=1&pro=&city=0&yys=0&port=11&time=1&ts=0&ys=0&cs=0&lb=1&sb=0&pb=4&mr=1&regions='
            r = urllib3.PoolManager().request('GET', url)
            if r.status == 200 and re.search('^[0-9\.:]+$', str(r.data.decode()).replace('\r\n', '')):
                return str(r.data.decode()).replace('\r\n', '')
            else:
                return None
        else:
            return None

    def complain(self, proxy, protocol='http'):
        pos = self.proxypooldb.getPos(proxy, protocol)
        if pos:
            self.proxypooldb.complain(pos, self.lifetime, protocol)
            if self.proxypooldb.getReputation(pos, protocol) < (self.fire_num - 2*self.fire_num):
                self.proxypooldb.deleteIndex(pos, protocol)

    def addProxy(self, protocol='http'):
        maxposBeforeLock = self.proxypooldb.getMaxPos(protocol)
        res = self.proxypooldb.lockPool(protocol)
        maxposAfterLock = self.proxypooldb.getMaxPos(protocol)
        if res and maxposBeforeLock == maxposAfterLock:
            pos = self.proxypooldb.newPos(protocol)
            proxy = self.getNewProxy(protocol)
            if proxy is not None:
                self.proxypooldb.addProxy(pos, proxy, self.lifetime, protocol)
                self.proxypooldb.addIndex(pos, self.lifetime, protocol)
                self.proxypooldb.addReputation(pos, self.lifetime, protocol)
            self.proxypooldb.releasePool(protocol)
            if proxy is not None:
                return pos
            else:
                logger.error('cat not get proxy from supplier')
                return None
        else:
            indexes = self.proxypooldb.getIndexes(protocol)
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