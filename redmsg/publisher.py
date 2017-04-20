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
from redis.client import StrictPipeline

class PublisherPipeline(StrictPipeline):

    def __init__(self, message_ttl, *args):
        super(PublisherPipeline, self).__init__(*args)
        self.message_ttl = message_ttl

    def publish(self, channel, message, ttl=None):
        return super(PublisherPipeline, self).publish('redmsg:' + channel, message)

class Publisher(object):

    def __init__(self, message_ttl=3600, **redis_config):
        self.redis = StrictRedis(**redis_config)
        self.message_ttl = message_ttl

    def pipeline(self, transaction=True, shard_hint=None):
        return PublisherPipeline(
            self.message_ttl,
            self.redis.connection_pool,
            self.redis.response_callbacks,
            transaction,
            shard_hint)

    def publish(self, channel, message, ttl=None):
        return self.redis.publish('redmsg:' + channel, message)
