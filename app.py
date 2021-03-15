#!/usr/bin/env python3

from environs import Env
import redis
from time import sleep

env = Env(expand_vars=True)
env.read_env()

REDIS_HOST = env.str('REDIS_HOST', '127.0.0.1')
REDIS_PASSWORD = env.str('REDIS_PASSWORD', None)
REDIS_PORT = env.int('REDIS_PORT', 6379)
REDIS_DB = env.int('REDIS_DB', 0)

class ScanRedisKeys(object):
  def __init__(self):
    self.init_client()

  def init_client(self):
    self.redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, db=REDIS_DB, decode_responses=True)

  def scan(self, key):
    finish = 0
    cursor = 0
    
    while(cursor or finish == 0):
      cursor, data = self.redis.scan(cursor, key)
      
      if (not cursor):
        finish = 1

      self.process(data)

  def process(self, data):
    raise NotImplementedError

  def get_keys(self, keys):
    if (isinstance(keys, str)):
      keys = [keys]

    return keys

  def run(self, keys):
    for key in self.get_keys(keys):
      print('run', key)
      self.scan(key)

class GiveRedisKeyExpireTime(ScanRedisKeys):
  def __init__(self, time=60):
    ScanRedisKeys.__init__(self)
    self.expire_time = time

  def process(self, data):
    for item in data:
      ttl = self.redis.ttl(item)

      print('expire %s' % (item))
      # 无过期时间的 key
      if (ttl == -1):
        self.redis.expire(item, self.expire_time)
      
      # 过期时间过长的 key
      if (ttl > self.expire_time):
        self.redis.expire(item, self.expire_time)

if __name__ == '__main__':
  key = [
    '*wx.session.key*',
    '*mina.member*',
    '*comments_avg_rating*',
  ]

  app = GiveRedisKeyExpireTime()
  app.run(key)
