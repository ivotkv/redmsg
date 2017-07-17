# -*- coding: utf-8 -*-
# 
# The MIT License (MIT)
# 
# Copyright (c) 2017 Ivo Tzvetkov
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
# 

from redis import StrictRedis
from .exc import *

__all__ = ['Publisher']

class Publisher(object):

    _publish_script = """
        local txid = redis.call('INCR', KEYS[1])
        redis.call('SETEX', KEYS[1] .. ':' .. txid, ARGV[1], ARGV[2])
        redis.call('PUBLISH', KEYS[1], txid .. ':' .. ARGV[2])
        return txid
    """

    def __init__(self, ttl=3600, redis=None, **redis_config):
        self.ttl = ttl
        self.redis = redis if redis is not None else StrictRedis(**redis_config)
        self._publish = self.redis.register_script(self._publish_script)

    def pipeline(self, transaction=True, shard_hint=None):
        pipeline = self.redis.pipeline(transaction=transaction, shard_hint=shard_hint)
        return PublisherPipeline(ttl=self.ttl, redis=pipeline)

    def publish(self, channel, message, ttl=None):
        return self._publish(('redmsg:' + channel,), (ttl or self.ttl, message))

    def txid(self, channel):
        txid = self.redis.get('redmsg:' + channel)
        return int(txid) if txid is not None else None

class PublisherPipeline(Publisher):

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.reset()

    def __len__(self):
        return len(self.redis)

    def reset(self):
        self.redis.reset()

    def execute(self, raise_on_error=True):
        return self.redis.execute(raise_on_error=raise_on_error)
